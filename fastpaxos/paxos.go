package fastpaxos

import (
	"github.com/ailidani/paxi/log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/ailidani/paxi"
)

const cleanupSlack int = 100
const entryCommandSetInitialCapacity int = 8

// entry in log at every node
type entry struct {
	ballot      paxi.Ballot
	command     paxi.Command
	commit 		bool
	fromClient  bool // whether this entry was created in fast mode by a message from the client
	quorum      *paxi.Quorum // we use this quorum for classical rounds: i.e. when the leader drives conflict resolution
	timestamp   time.Time
}

// Paxos instance
type FastPaxos struct {
	paxi.Node
	nodeCount int // total number of nodes. This should be pulled from config.N()

	log             map[int]*entry // log ordered by slot. This has one command in every slot
	resolutionLog   map[int]*CommandSet // log of commandSet with votes. This contains all cmds a leader has received in a slot
	p1Resolutions   map[int]*CommandSet // log of commandSet with commands learned in phase-1

	execute       int         // next execute slot number
	slot          int         // highest slot number
	ballot        paxi.Ballot // highest ballot number
	conflictTotal int         // count total number of slow path recoveries we have done. Keep for stats only
	active        bool        // active leader
	IsFastMode    bool

	pendingCmds   *CommandSet // keeps all commands pending fast quorum with # of times each cmd is observed in slots that cannot reach fast quorum
	decidedCmds   *CommandSet // keeps the decided commands to prevent adding decided cmd to pending list
	pendingReply  *CommandSet // keeps decided commands for which we have not replied yet because of no client request
	resolvingCmds *CommandSet // keeps all commands currently being resolved after not reaching a fast quorum
	conflictSlots map[int]bool

	quorum   		*paxi.Quorum    // phase 1 quorum
	requests 		map[UniqueCommandId]*paxi.Request // requests that have not been answered yet per command in fast mode
	pendingRequests []*paxi.Request // pending requests we accumulate when (1) not yet a leader, (2) not yet in fast mode

	p1Scheduled bool // whether we have scheduled a timer for a p1. The timer can be canceled if a leader emerges
	p1Timer     *time.Timer
	p1Cancel    chan struct{}

	Q1              func(*paxi.Quorum) bool
	Q2c             func(*paxi.Quorum) bool
	Q2f             func(*paxi.Quorum) bool
	Q1Size			int
	Q2fSize			int
	Q2cSize			int

	ReplyWhenCommit bool

	sync.RWMutex
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(paxos *FastPaxos)) *FastPaxos {
	log.Infof("FastPaxos is starting on node %v.", n.ID(),)
	p := &FastPaxos{
		Node:            n,
		nodeCount:       paxi.GetConfig().N(),
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make(map[UniqueCommandId]*paxi.Request, paxi.GetConfig().BufferSize),
		ReplyWhenCommit: false,
		pendingRequests: make([]*paxi.Request, 0),
	}

	if *p1q == 0 {
		p.Q1 = func(q *paxi.Quorum) bool { return q.Majority() }
		p.Q1Size = p.quorum.MajoritySize()
	} else {
		p.Q1 = func(q *paxi.Quorum) bool { return q.Size() >= *p1q }
		p.Q1Size = *p1q
	}

	if *p2qc== 0 {
		p.Q2c = func(q *paxi.Quorum) bool { return q.Majority() }
		p.Q2cSize = p.quorum.MajoritySize()
	} else {
		p.Q2c = func(q *paxi.Quorum) bool { return q.Size() >= *p2qc }
		p.Q2cSize = *p2qc
	}

	if *p2qf == 0 {
		p.Q2f = func(q *paxi.Quorum) bool { return q.FastQuorum() }
		p.Q2fSize = p.quorum.FastQuorumSize()
	} else {
		p.Q2f = func(q *paxi.Quorum) bool { return q.Size() >= *p2qf }
		p.Q2fSize = *p2qf
	}

	for _, opt := range options {
		opt(p)
	}

	p.startLeaderElectionWhenUp()

	log.Infof("FastPaxos constructor done on node %v", n.ID())
	return p
}

func (p *FastPaxos) startLeaderElectionWhenUp() {
	ticker := time.NewTicker(time.Duration(500 + p.ID().Node() * 50) * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				log.Infof("Initial Election Over")
				return
			case <-ticker.C:
				if p.TryConnectToKnownAddresses() {
					log.Infof("Initial Election Ticker. Nodes Up: %d", p.CountKnownNodes())
					if p.CountKnownNodes() == len(paxi.GetConfig().Addrs) {
						if p.ballot.N() == 0 {
							p.P1a()
						}
						ticker.Stop()
						done <- true
					}
				}
			}
		}
	}()
}

// IsLeader indicates whether this node is current leader
func (p *FastPaxos) IsLeader() bool {
	return p.active || p.ballot.ID() == p.ID()
}

// Leader returns leader id of the current ballot
func (p *FastPaxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *FastPaxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetBallot sets a new ballot number
func (p *FastPaxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// HandleRequest handles request and start phase 1 or phase 2
func (p *FastPaxos) HandleRequest(r paxi.Request) {
	log.Debugf("Replica %s on ballot %v and slot %d received %v\n", p.ID(), p.ballot, p.slot, r)

	// there is a chance this command has already been executed. This is because the request from the client may have
	// arrived to the leader later than to the followers.
	// due to current paxi limitations, we can reply to the client only when receiving a request (which is now)
	if p.pendingReply != nil && p.pendingReply.Exists(&r.Command) {
		p.requests[UniqueCommandIdFromCommand(&r.Command)] = &r
		// we have already tried to reply to the command, but failed because of not having the request
		log.Debugf("Attempting a pending reply for cmd %v", r.Command)
		cmdId := UniqueCommandIdFromCommand(&r.Command)
		p.reply(p.pendingReply.Get(cmdId))
		p.pendingReply.Remove(&r.Command)
	}

	if p.IsFastMode {
		// in FastBallot we receive requests form clients directly
		// and accept them
		log.Debugf("Replica %s on ballot %v handles request in Fast Mode %v\n", p.ID(), p.ballot)
		p2aMsg := P2a{Ballot: p.ballot, Slot: p.advanceSlot(), Command: r.Command, fromClient: true}
		if p.IsLeader() {
			p.requests[UniqueCommandIdFromCommand(&r.Command)] = &r
			p.HandleP2aFastLeader(p2aMsg)
		} else {
			p.HandleP2aFollower(p2aMsg)
			log.Debugf("Replica %s Reply non-leader on request %v\n", p.ID(), r)
			reply := paxi.Reply{
				Command:    r.Command,
				Value:      paxi.Value{},
				Properties: make(map[string]string),
			}

			reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
			r.Reply(reply)
		}
	} else {
		// if we are not in fast mode, then we need to wait to go into fast mode before processing requests
		log.Debugf("Adding request %r to pendingRequests for resolution after the leader election", r)
		p.pendingRequests = append(p.pendingRequests, &r)
		// however, if we are not active, and current ballot is not ours (i.e. we are not trying P1), then we
		// can schedule P1
		if !p.active && p.ID() != p.ballot.ID() {
			if !p.p1Scheduled {
				p.p1Cancel = make(chan struct{})
				r := rand.Intn(100)
				p.p1Timer = time.NewTimer(time.Duration(500+r) * time.Millisecond)
				go func() {
					select {
					case <-p.p1Cancel:
						log.Infof("Node %v p1 timer cancelled", p.ID())
						p.p1Scheduled = false
						return
					case <-p.p1Timer.C:
						p.p1Scheduled = false
						p.P1a()
					}
				}()
			}
		}
	}
}

// P1a starts phase 1 prepare
func (p *FastPaxos) P1a() {
	if p.active {
		return
	}
	p.p1Resolutions = make(map[int]*CommandSet)
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.Broadcast(P1a{Ballot: p.ballot})

	// self vote. broadcast excludes self
	p.IsFastMode = false // new ballot, reset fast flag
	p.quorum.ACK(p.ID()) // vote
	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{Command: p.log[s].command, Ballot: p.log[s].ballot}
	}

	// handle p1b from self
	p.HandleP1b(P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})

}

// P2a starts phase 2 accept
func (p *FastPaxos) P2a(r *paxi.Request) {
	slot := p.advanceSlot()
	p.log[slot] = &entry{
		ballot:    p.ballot,
		command:   r.Command,
		timestamp: time.Now(),
		quorum:    paxi.NewQuorum(),
	}
	p.log[slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		Slot:        slot,
		Command:     r.Command,
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

func (p *FastPaxos) P2aAny() {
	slot := p.advanceSlot()
	cmd := paxi.Command{CommandID: -1}
	p.log[slot] = &entry{
		ballot:      p.ballot,
		command:	 cmd,
		timestamp:   time.Now(),
		quorum:      paxi.NewQuorum(),
	}
	p.log[slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		Slot:        slot,
		Command:     cmd,
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// Resolves some conflicting slot with a command
func (p *FastPaxos) P2aResolveSlot(cmd *paxi.Command, slot int) {
	log.Debugf("Replica %s sending P2a for classical round resolution", p.ID())

	p.log[slot] = &entry{
		ballot:      p.ballot,
		command:     *cmd,
		timestamp:   time.Now(),
		quorum:		 paxi.NewQuorum(),
	}
	p.log[slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		Slot:        slot,
		Command:     *cmd,
	}

	// remove from pending, since we no longer wait for this command to get to fast quorum
	p.pendingCmds.Remove(cmd)

	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// HandleP1a handles P1a message
func (p *FastPaxos) HandleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.IsFastMode = false // new ballot, reset fast flag
	}

	l := make(map[int]CommandBallot)
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{Command: p.log[s].command, Ballot: p.log[s].ballot}
	}

	p.Send(m.Ballot.ID(), P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
}

func (p *FastPaxos) update(scb map[int]CommandBallot, nodeId paxi.ID) {
	for slot, CmdBallot := range scb {
		p.slot = paxi.Max(p.slot, slot)

		if cs, exists := p.p1Resolutions[slot]; exists {
			cs.AddWithVote(&CmdBallot.Command, nodeId)
		} else {
			cs := NewCommandSet(entryCommandSetInitialCapacity, true, false)
			cs.AddWithVote(&CmdBallot.Command, nodeId)
			p.p1Resolutions[slot] = cs
		}
	}
}

// HandleP1b handles P1b message
func (p *FastPaxos) HandleP1b(m P1b) {
	// old message
	if m.Ballot < p.ballot || p.active {
		log.Debugf("Replica %s ignores old message [%v]\n", p.ID(), m)
		return
	}
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())
	// reject message because ballot has changed
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false     // not necessary
		p.IsFastMode = false // new ballot, reset fast flag
	}
	p.update(m.Log, m.ID) // add all slots learned in phase-1 to the resolution log

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			log.Debugf("Replica %s becomes active\n", p.ID())
			if p.p1Scheduled {
				p.p1Cancel <- struct{}{}
			}
			p.active = true
			p.pendingCmds = NewCommandSet(paxi.GetConfig().BufferSize, true, true)
			p.decidedCmds = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
			p.resolvingCmds = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
			p.pendingReply = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
			p.conflictSlots = make(map[int]bool, paxi.GetConfig().BufferSize)
			p.resolutionLog = make(map[int]*CommandSet, paxi.GetConfig().BufferSize)
			// some of our pending requests may be in slots we are about to recover
			// so we need to make sure we can reply on these
			for _, pendingReq := range p.pendingRequests {
				log.Debugf("Adding request %v from pendingRequests to a request set for reply", pendingReq)
				p.requests[UniqueCommandIdFromCommand(&pendingReq.Command)] = pendingReq
			}

			// recover entries
			for i := p.execute; i <= p.slot; i++ {
				cs := p.p1Resolutions[i]
				cmd :=  *cs.commandSet[cs.maxUCId]
				p.log[i] = &entry{
					ballot:     p.ballot,
					command:    cmd,
					commit:     false,
					fromClient: false,
					quorum:     paxi.NewQuorum(),
					timestamp:  time.Now(),
				}

				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:      p.ballot,
					Slot:        i,
					Command:     cmd, // this is the command with max number of votes
				})
			}
			// propose ANY
			p.P2aAny()

			// periodically re-elect the leader, to sync all nodes to the same slot number and resolve any unresolved slots
			log.Debugf("Node %v is scheduling a re-election timer", p.ID())

			if *reelectiontime > 0 {
				reelectionTimer := time.NewTimer(time.Duration(*reelectiontime) * time.Millisecond)
				go func() {
					<-reelectionTimer.C
					log.Debugf("Re-election triggered on node %v", p.ID())
					p.active = false
					p.P1a()
				}()
			}
		}
	}
}

// HandleP2aFollower handles P2a message
func (p *FastPaxos) HandleP2aFollower(m P2a) {
	log.Debugf("HandleP2aFollower on %s, p2a: %v\n", p.ID(), m)

	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		if m.Ballot > p.ballot {
			p.IsFastMode = false // new ballot, reset fast flag
		}
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && (m.Ballot > e.ballot || (m.Ballot == e.ballot && e.fromClient)) {
				// different command on the same slot. This usually means a recovery from leader
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:      m.Ballot,
				command:     m.Command,
				commit:      false,
				fromClient:  m.fromClient, // record the flag on whether this message came from the leader or client
			}
		}

		// if this is a classical quorum, set our slot to max of ours and theirs
		if !p.IsFastMode && !m.fromClient {
			log.Debugf("Setting a slot to max of our(%d) and leader(%d)", p.slot, m.Slot)
			p.slot = paxi.Max(p.slot, m.Slot)
		}
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot:        p.ballot,
		Command:       m.Command,
		Slot:          m.Slot,
		ID:            p.ID(),
		ClassicQuourm: !m.fromClient,
	})
}

// HandleP2aFastLeader coming from user request. this should not reset active status on leader
func (p *FastPaxos) HandleP2aFastLeader(m P2a) {
	log.Debugf("HandleP2aFastLeader on %s, p2a: %v\n", p.ID(), m)

	if m.Ballot == p.ballot && p.IsLeader() {
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			// we received a request from a client, but we already put have something in this slot number
			// if this message has a higher ballot, and slot is not committed, we need to update the slot
			// if the ballots are the same, then we could have put stuff in the log in p2b is when a quorum has
			// been reached (fast or classic) so this means the command must have been committed.
			// If it is not committed, we have a problem.

			if !e.commit && m.Ballot > e.ballot {
				log.Debugf("Resetting a slot at the leader due to a client msg with higher ballot")
				e.command = m.Command
				e.ballot = m.Ballot
			} else if e.commit && m.Ballot > e.ballot && !m.Command.Equal(&e.command) {
				log.Errorf("Trying to overwrite committed slot with a higher ballot message")
			} else if !e.commit {
				log.Errorf("Trying to update uncommitted slot without having a higher ballot")
			}
		} else {
			log.Debugf("Replica %s HandleP2aFastLeader adding command %v to log on slot %d \n", p.ID(), m.Command, m.Slot)
			p.log[m.Slot] = &entry{
				ballot:      m.Ballot,
				command:     m.Command,
				commit:      false,
				quorum:      paxi.NewQuorum(),
				timestamp:   time.Now(),
			}
		}

		// leader votes with P2b on this
		p2b := P2b{ID:p.ID(), Ballot: m.Ballot, Command: m.Command, Slot:m.Slot}
		p.HandleP2b(p2b)

	} else if m.Ballot > p.ballot {
		p.active = false
		p.IsFastMode = false
	}
}

// HandleP2b handles P2b message
func (p *FastPaxos) HandleP2b(m P2b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	if m.Ballot < p.ballot {
		// ignore old message
		log.Debugf("Replica %s ignores old message %v", p.ID(), m)
		return
	}

	// P2b reject message
	// node updates its ballot number and falls back to the acceptor state
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.IsFastMode = false
	}

	logentry, exist := p.log[m.Slot]
	if !m.ClassicQuourm {
		log.Debugf("Replica %s Adding %v to the commandSet on slot %d\n", p.ID(), m, m.Slot)
		// add the command to a commandSet on this slot
		// Slot's command set counts how many times each command has been seen
		// it also count total number of nodes replying on this slot so far
		var cmdQuorum *paxi.Quorum
		if cs, exists := p.resolutionLog[m.Slot]; exists {
			cmdQuorum, _, _, _ = cs.AddWithVote(&m.Command, m.ID)
		} else {
			cs := NewCommandSet(entryCommandSetInitialCapacity, true, false)
			cmdQuorum, _, _, _ = cs.AddWithVote(&m.Command, m.ID)
			p.resolutionLog[m.Slot] = cs
		}

		if cmdQuorum == nil {
			log.Errorf("A vote could not be applied to a command %v", m.Command)
			return
		}

		// ensure the command is in the pending set only if it has not been decided or currently resolving
		if !p.decidedCmds.Exists(&m.Command) && !p.resolvingCmds.Exists(&m.Command) {
			if exist && logentry.commit {
				// even if we already have such slot committed (but the command), we may want to count this cmd for purposes
				// of figuring out the future resolutions
				p.pendingCmds.AddWithVote(&m.Command, m.ID)
				// check if we can recover any of the undecided slots that have conflict
				p.recoverSlotsIfPossible()
				// ignoring already committed slot
				return
			} else {
				p.pendingCmds.Add(&m.Command)
			}
			p.pendingCmds.AddSlotQuorumIfDoesNotExist(&m.Command, m.Slot, cmdQuorum)
		}

		if exist && logentry.commit {
			return // we already have something committed on this slot, so no need to go further
		}

		if p.Q2f(cmdQuorum) {
			// we have a fast quorum on this slot
			p.log[m.Slot] = &entry{
				ballot:      m.Ballot,
				command:     m.Command,
				commit:      true,
				quorum:      cmdQuorum,
				timestamp:   time.Now(),
			}

			p.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: m.Command,
			})
			p.pendingCmds.Remove(&m.Command) // remove from pending set
			p.decidedCmds.Add(&m.Command)    // add to the decided set

			delete(p.conflictSlots, m.Slot) // this slot should not be in conflicts, but clean up just in case

			// Add counts for each command to the pending counts.
			p.pendingCmds.AddVotesFromAnotherCommandSet(p.resolutionLog[m.Slot], p.Q2fSize)

			// now check if we can recover any of the undecided slots that have conflict
			p.recoverSlotsIfPossible()

			if p.ReplyWhenCommit {
				p.replyOnCommit(&m.Command)
			} else {
				p.exec()
			}

			return // we are done here
		}

		if !p.isFastQuorumPossible(m.Slot) {
			log.Debugf("No fast quorum is possible on slot %d", m.Slot)
			p.pendingCmds.AddVotesFromAnotherCommandSet(p.resolutionLog[m.Slot], p.Q2fSize)
			// Add the slot to conflicts slot with false for "not yet recovering"
			if _, exists := p.conflictSlots[m.Slot]; !exists {
				p.conflictSlots[m.Slot] = false
			}
			p.recoverSlotsIfPossible()
		}

	} else {
		log.Debugf("Replica %s in ballot %s received p2b %v in classical round", p.ID(), p.ballot, m)
		// regular round
		entry, exist := p.log[m.Slot]
		if !exist /*|| m.Ballot < entry.ballot*/ || entry.commit {
			return
		}

		if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
			log.Debugf("Replica %s in ballot %s applying vote to quorum %v", p.ID(), p.ballot, p.log[m.Slot].quorum.Size())
			p.log[m.Slot].quorum.ACK(m.ID)
			if p.Q2c(p.log[m.Slot].quorum) {

				p.Broadcast(P3{
					Ballot:  m.Ballot,
					Slot:    m.Slot,
					Command: p.log[m.Slot].command,
				})
				p.log[m.Slot].commit = true
				p.pendingCmds.Remove(&m.Command)   // remove from pending set if it is there (it should not be!)
				p.resolvingCmds.Remove(&m.Command) // remove from resolving set
				p.decidedCmds.Add(&m.Command)      // add to the decided set
				delete(p.conflictSlots, m.Slot)    // clean up conflicts is the slot is there
				if p.ReplyWhenCommit {
					p.replyOnCommit(&p.log[m.Slot].command)
				} else {
					p.exec()
				}
			}
		}
	}
}

/**
* HandleP3 handles phase 3 commit message
* here we assume P3 has a copy of command. This is not super efficient, but it is the same assumption we make in
* Multi-Paxos in Paxi. Will fix in a rewrite of Paxi
*/
func (p *FastPaxos) HandleP3(m P3) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// P3 carries a quorum-committed cmd, so we can overwrite it here even if our command in the slot is different
	e, exist := p.log[m.Slot]
	if !exist {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}

	e.command = m.Command
	e.commit = true
	e.ballot = m.Ballot

	if p.ReplyWhenCommit {
		p.replyOnCommit(&m.Command)
	} else {
		p.exec()
	}
}

func (p *FastPaxos) recoverSlotsIfPossible() {
	log.Debugf("Pending cmds {%v} and count {%v}", p.pendingCmds.commandSet, p.pendingCmds.quorums)
	for slot, resolving := range p.conflictSlots {
		if !resolving {
			if cs, exists := p.resolutionLog[slot]; exists {
				// get a set of commands on this slot that are not still pending fast quorum and not resolving in some other slot
				resolveCandidates := cs.Intersect(p.pendingCmds).Difference(p.resolvingCmds)
				log.Debugf("Checking whether slot %d can be recovered with possible commands: %v", slot, resolveCandidates)
				if len(resolveCandidates.commandSet) == 1 {
					// when we have only one command left in the intersection of slot's commands and pending
					// we can recover that command in that slot. The recovery is a P2a from the leader
					for _, cmd := range resolveCandidates.commandSet {
						if cmd != nil {
							log.Infof("Resolving command %v for slot %d", cmd, slot)
							p.P2aResolveSlot(cmd, slot)
							p.conflictSlots[slot] = true
							p.resolvingCmds.Add(cmd)
						}
					}
				} else {
					n := paxi.GetConfig().N()
					var maxVotedCmdForRecovery *paxi.Command
					maxVotes := 0
					for _, cmd := range resolveCandidates.commandSet {
						ucid := UniqueCommandIdFromCommand(cmd)
						responsesSoFar := p.pendingCmds.GetQuorum(ucid).Size()
						if n - responsesSoFar < p.Q2fSize {
							// no fast quorum is possible for this command
							v := cs.GetQuorum(ucid).Size()
							if v > maxVotes {
								maxVotes = v
								maxVotedCmdForRecovery = cmd
							}
						}
					}
					if maxVotes > 0 {
						// we have a command that cannot reach fast quorum that needs a recovery
						log.Infof("Resolving command %v for slot %d", maxVotedCmdForRecovery, slot)
						p.P2aResolveSlot(maxVotedCmdForRecovery, slot)
						p.conflictSlots[slot] = true
						p.resolvingCmds.Add(maxVotedCmdForRecovery)
					}
				}
			}
		} else {
			log.Debugf("Slot %d is already resolving. Skipping", slot)
		}
	}
}

func (p *FastPaxos) isFastQuorumPossible(slot int) bool {
	log.Debugf("Checking whether fast quorum is possible on slot %d: n=%d, Q2fSize=%d",slot, p.nodeCount, p.Q2fSize)
	if cs, exists := p.resolutionLog[slot]; exists {
		// if we have enough empty spots in the slot to get fast quorum, then quorum is still possible
		if p.nodeCount - cs.totalVotes.Size() >= p.Q2fSize {
			return true
		}
		for cmdid, _ := range cs.commandSet {
			if p.pendingCmds.ExistsByUCID(cmdid) {
				slotTotal := cs.totalVotes.Clone()
				cmdQs := p.pendingCmds.slotQuorums[cmdid]
				log.Debugf("------------------------------------------%v: %v", cmdid, cmdQs)

				// fast quorum is still possible on command when
				// (1) remaining votes => number of votes to reach quorum
				// (2) other slots have not taken possible votes for the command
				// so below we take the copy of total count on the slot, and add the votes for this command from other slot.
				// this way we fill up the empty vote positions and can really count how many more votes we can realyl expect
				for _, q := range cmdQs {
					slotTotal.AddFromQuorum(q)
				}
				if p.nodeCount-slotTotal.Size() >= p.Q2fSize-cmdQs[slot].Size() {
					return true
				}
			}
		}
	}

	return false
}

func (p *FastPaxos) exec() {
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.command.CommandID == -1 {
			// we use CommandID -1 as a marker for a command that enables fast mode
			p.IsFastMode = true
			// we can do a list of pending requests now, since we are in the fast mode
			p.applyPendingRequests()
		} else {
			e.command.Value = value
			p.reply(&e.command)
		}
		// TODO clean up the log periodically instead of using some slack value
		// using this slack is dangerous since we do not know if all nodes are actually within the slack of each other
		// and that no recovery will be needed
		if p.execute > cleanupSlack {
			if _, exists := p.resolutionLog[p.execute - cleanupSlack]; exists {
				delete(p.resolutionLog, p.execute - cleanupSlack)
			}
			if p.decidedCmds != nil {
				p.decidedCmds.Remove(&p.log[p.execute-cleanupSlack].command)
			}
			delete(p.log, p.execute - cleanupSlack)
		}
		p.execute++
	}
}

func (p *FastPaxos) replyOnCommit(cmd *paxi.Command) {
	log.Debugf("Replica %s replyOnCommit [s=%d, cmd=%v]", p.ID(), cmd)
	p.reply(cmd)
}

func (p *FastPaxos) reply(cmd *paxi.Command) {
	cmdId := UniqueCommandIdFromCommand(cmd)
	if req, exists := p.requests[cmdId]; exists {
		log.Debugf("Replica %s replies on command %v\n", p.ID(), *cmd)
		reply := paxi.Reply{
			Command:    *cmd,
			Value:      cmd.Value,
			Properties: make(map[string]string),
		}
		reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
		req.Reply(reply)
		delete(p.requests, cmdId)
	} else if p.IsLeader() {
		// current Paxi limitations do not allow replying to a client without receiving a request first
		// add command for pending reply set, so we can reply to it once the leader receives the client request
		log.Debugf("Replica %s records a pending reply on command %v\n", p.ID(), *cmd)
		p.pendingReply.Add(cmd)
	}
}

func (p *FastPaxos) advanceSlot() int {
	p.slot += 1
	return p.slot
}

func (p *FastPaxos) applyPendingRequests() {
	if p.active || p.IsFastMode {
		for _, r := range p.pendingRequests {
			p.HandleRequest(*r)
		}
	}
	p.pendingRequests = make([]*paxi.Request, 0)
}