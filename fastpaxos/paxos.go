package fastpaxos

import (
	"bytes"
	"github.com/ailidani/paxi/log"
	"strconv"
	"sync"
	"time"

	"github.com/ailidani/paxi"
)

const cleanupSlack int = 100

// entry in log
type entry struct {
	ballot      paxi.Ballot
	conflictNum int  // we use this as a flag to see if we have done a slow path recovery on this entry
	command     paxi.Command
	commit      bool
	quorum      *paxi.Quorum
	timestamp   time.Time
}

type p2vote struct {
	Cmd paxi.Command
	Q	*paxi.Quorum
}

// Paxos instance
type FastPaxos struct {
	paxi.Node

	log             map[int]*entry // log ordered by slot
	learnerlog      map[int]*paxi.Command // log of KVs to reply to client by leader
	p2bCommandVotes map[int][]*p2vote

	execute       int         // next execute slot number
	active        bool        // active leader
	ballot        paxi.Ballot // highest ballot number
	conflictTotal int         // count total number of slow path recoveries we have done
	IsFastBallot  bool
	slot          int         // highest slot number

	quorum   		*paxi.Quorum    // phase 1 quorum
	requests 		map[int64]*paxi.Request // pending requests that have not been answered yet

	Q1              func(*paxi.Quorum) bool
	Q2c             func(*paxi.Quorum) bool
	Q2f             func(*paxi.Quorum) bool

	Q1Size			int
	Q2fSize			int
	Q2cSize			int

	useClientSlots  bool // we use this for testing. Let clients set the slots to control the rate of conflict

	ReplyWhenCommit bool

	sync.RWMutex
}

// NewPaxos creates new paxos instance
func NewPaxos(n paxi.Node, options ...func(paxos *FastPaxos)) *FastPaxos {
	log.Infof("FastPaxos is starting on node %v. Use ClientSlots: %v", n.ID(), *clientslots)
	p := &FastPaxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		learnerlog:      make(map[int]*paxi.Command, paxi.GetConfig().BufferSize),
		p2bCommandVotes: make(map[int][]*p2vote),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make(map[int64]*paxi.Request, paxi.GetConfig().BufferSize),
		ReplyWhenCommit: false,
		useClientSlots:  *clientslots,
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
	p.cleanUpTicker()

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

func (p *FastPaxos) cleanUpTicker() {
	ticker := time.NewTicker(time.Duration(paxi.GetConfig().ClientTimeout) * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if p.IsLeader() {
					p.enforceTimeoutOnAllOutstandingRequests()
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
	log.Debugf("Replica %s on ballot %v received %v\n", p.ID(), p.ballot, r)
	tsReq := time.Now().UnixNano()
	p.Lock()
	p.requests[tsReq] = &r
	p.Unlock()
	if p.IsFastBallot {
		// in FastBallot we receive requests form clients directly
		// and accept them
		var p2aMsg P2a
		if p.useClientSlots {
			p2aMsg = P2a{Ballot: p.ballot, ConflictNum: 0, Slot: r.Command.CommandID, Command: r.Command}
		} else {
			p2aMsg = P2a{Ballot: p.ballot, ConflictNum: 0, Slot: p.slot + 1, Command: r.Command}
		}
		if p.IsLeader() {
			p.HandleP2aFastLeader(p2aMsg, &r, tsReq)
		} else {
			p.HandleP2a(p2aMsg)
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
		if !p.active {
			// current phase 1 pending
			if p.ballot.ID() != p.ID() {
				p.P1a()
			}
		} else {
			p.P2a(&r)
		}
	}
}

// P1a starts phase 1 prepare
func (p *FastPaxos) P1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.Broadcast(P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *FastPaxos) P2a(r *paxi.Request) {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:      p.ballot,
		conflictNum: 0,
		command:     r.Command,
		quorum:      paxi.NewQuorum(),
		timestamp:   time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		ConflictNum: 0,
		Slot:        p.slot,
		Command:     r.Command,
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

func (p *FastPaxos) P2aAny() {
	p.slot++
	p.log[p.slot] = &entry{
		ballot:      p.ballot,
		conflictNum: 0,
		command:     paxi.Command{CommandID: -1},
		quorum:      paxi.NewQuorum(),
		timestamp:   time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		ConflictNum: 0,
		Slot:        p.slot,
		Command:     paxi.Command{CommandID: -1},
	}
	if paxi.GetConfig().Thrifty {
		p.MulticastQuorum(paxi.GetConfig().N()/2+1, m)
	} else {
		p.Broadcast(m)
	}
}

// P2a starts phase 2 accept
func (p *FastPaxos) P2aRetry(cmd *paxi.Command, slot int) {
	log.Debugf("Replica %s sending P2a for classical round resolution", p.ID())

	p.log[slot] = &entry{
		ballot:      p.ballot,
		conflictNum: p.conflictTotal,
		command:     *cmd,
		quorum:      paxi.NewQuorum(),
		timestamp:   time.Now(),
	}
	p.log[slot].quorum.ACK(p.ID())
	m := P2a{
		Ballot:      p.ballot,
		ConflictNum: p.conflictTotal,
		Slot:        slot,
		Command:     *cmd,
	}
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
		p.IsFastBallot = false // new ballot, reset fast flag
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

func (p *FastPaxos) update(scb map[int]CommandBallot) {
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.conflictNum = 0
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:      cb.Ballot,
				conflictNum: 0,
				command:     cb.Command,
				commit:      false,
			}
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

	p.update(m.Log)

	// reject message because ballot has changed
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false // not necessary
		p.IsFastBallot = false // new ballot, reset fast flag
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			log.Debugf("Replica %s becomes active\n", p.ID())
			p.active = true
			// propose any uncommitted entries
			for i := p.execute; i <= p.slot; i++ {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:      p.ballot,
					ConflictNum: 0,
					Slot:        i,
					Command:     p.log[i].command,
				})
			}
			// propose ANY
			p.P2aAny()
		}
	}
}

// HandleP2a handles P2a message
func (p *FastPaxos) HandleP2a(m P2a) {
	log.Debugf("HandleP2a on %s, p2a: %v\n", p.ID(), m)

	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.conflictTotal = m.ConflictNum
		p.active = false
		if m.Ballot > p.ballot {
			p.IsFastBallot = false // new ballot, reset fast flag
		}
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && (m.Ballot > e.ballot || (m.Ballot == e.ballot && m.ConflictNum > e.conflictNum)) {
				// different command
				e.command = m.Command
				e.ballot = m.Ballot
				e.conflictNum = m.ConflictNum
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:      m.Ballot,
				conflictNum: m.ConflictNum,
				command:     m.Command,
				commit:      false,
			}
		}

		if m.Command.CommandID == -1 {
			p.IsFastBallot = true
		}
	}

	p.Send(m.Ballot.ID(), P2b{
		Ballot:     p.ballot,
		ConflicNum: m.ConflictNum,
		Command:    m.Command,
		Slot:       m.Slot,
		ID:         p.ID(),
	})
}

// HandleP2a coming from user request. this should not reset active status on leader
func (p *FastPaxos) HandleP2aFastLeader(m P2a, r *paxi.Request, tsReq int64) {
	log.Debugf("HandleP2aFastLeader on %s, p2a: %v\n", p.ID(), m)

	if m.Ballot == p.ballot && p.IsLeader() {
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				e.command = m.Command
				e.ballot = m.Ballot
				e.conflictNum = 0
			} else if e.commit && p.useClientSlots && e.command.Key != r.Command.Key {
				reply := paxi.Reply{
					Command:    r.Command,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPLostSlot] = "true"
				reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
				r.Reply(reply)
				return
			}
		} else {
			log.Debugf("Replica %s HandleP2aFastLeader adding command %v to log \n", p.ID(), m.Command)
			p.log[m.Slot] = &entry{
				ballot:      m.Ballot,
				conflictNum: 0,
				command:     m.Command,
				commit:      false,
				quorum:      paxi.NewQuorum(),
				timestamp:   time.Now(),
			}
		}

		for s, llog := range p.learnerlog {
			if llog.CommandID == m.Command.CommandID && llog.Key == m.Command.Key {
				// we found the learnerlog to reply
				log.Debugf("Replica %s Reply on slot %d from learnedLog command %v\n", p.ID(), s, llog)
				reply := paxi.Reply{
					Command:    *llog,
					Value:      llog.Value,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
				r.Reply(reply)
				delete(p.learnerlog, s)
				p.Lock()
				delete(p.requests, tsReq)
				p.Unlock()
				break
			}
		}

		p2b := P2b{ID:p.ID(), Ballot: m.Ballot, ConflicNum: 0, Command: m.Command, Slot:m.Slot}
		p.HandleP2b(p2b)

	} else if m.Ballot > p.ballot {
		p.active = false
		p.IsFastBallot = false
	}
}

// HandleP2b handles P2b message
func (p *FastPaxos) HandleP2b(m P2b) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.ID, m, p.ID())

	// reject message
	// node update its ballot number and falls back to the acceptor state
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.IsFastBallot = false
	}

	logentry, exist := p.log[m.Slot]
    if exist && logentry.commit {
    	// ignoring already committed entry
    	return
	}

	if p.IsFastBallot && m.ConflicNum == 0 {
		// add this P2b to list of P2bs we have collected so far on this slot
		log.Debugf("Replica %s Adding P2b %v to p2bCommandVotes %d\n", p.ID(), m, m.Slot)
		p2votes, p2bsExist := p.p2bCommandVotes[m.Slot]
		if !p2bsExist {
			p.p2bCommandVotes[m.Slot] = make([]*p2vote, 0)
		}

		voteApplied := false
		quorumMet := false

		// loop through all unique operations we have in this slot so far, and apply a vote if msg m contains one
		// of these operations. Then count quorum on the operation and see we have reaches fast quorum
		// here we assume operations equality as having the same key and value
		for _, p2v := range p2votes {
			if p2v.Cmd.Key == m.Command.Key && bytes.Equal(p2v.Cmd.Value, m.Command.Value) {
				p2v.Q.ACK(m.ID)
				log.Debugf("Replica %s applying vote for msg %v on slot %d for a total of %d\n", p.ID(), m, m.Slot, p2v.Q.Size())
				voteApplied = true
				if p.Q2f(p2v.Q) {
					quorumMet = true
					// we have a fast quorum on this slot
					p.log[m.Slot] = &entry{
						ballot:      m.Ballot,
						conflictNum: 0,
						command:     m.Command,
						commit:      true,
						quorum:      p2v.Q,
						timestamp:   time.Now(),
					}

					if p.ReplyWhenCommit {
						p.replyOnCommit(p2v.Cmd)
					} else {
						p.exec()
					}
				}
			}
		}

		// the operation in msg m was not something we have seen yet, so add it to the list of votes with 1 vote
		if !voteApplied {
			p2v := p2vote{Cmd: m.Command, Q: paxi.NewQuorum()}
			p2v.Q.ACK(m.ID)
			log.Debugf("Replica %s applying vote for msg %v on never before seen slot %d for a total of %d\n", p.ID(), m, m.Slot, p2v.Q.Size())
			p.p2bCommandVotes[m.Slot] = append(p.p2bCommandVotes[m.Slot], &p2v)
		}

		// finally, if we have no quorum after processing msg m, we check if there is still a chance to
		// get to a fast quorum. If fast quorum is no longer possible, we start recovery immediately.
		// for recovery we pick the value with the most votes
		if !quorumMet && (!exist || (exist && logentry.conflictNum == 0)) {
			if p.hasConflict(m.Slot) {
				// do conflict round
				cmd := p.p1bResultFromP2bFast(m.Slot)
				if cmd != nil {
					log.Infof("Conflict detected on slot %d, starting classical round to resolve. cmd: %v", m.Slot, cmd)
					p.conflictTotal++
					p.P2aRetry(cmd, m.Slot)
				}
			}
		}

	} else {
		log.Debugf("Replica %s in ballot %s received p2b %v in classical round", p.ID(), p.ballot, m)
		// regular round
		entry, exist := p.log[m.Slot]
		if !exist /*|| m.Ballot < entry.ballot*/ || entry.commit {
			return
		}

		log.Debugf("Replica %s has entry %v in slot %d", p.ID(), entry.command, m.Slot)

		if m.Ballot.ID() == p.ID() && m.Ballot == p.log[m.Slot].ballot {
			p.log[m.Slot].quorum.ACK(m.ID)
			if p.Q2c(p.log[m.Slot].quorum) {
				p.log[m.Slot].commit = true
				if p.ReplyWhenCommit {
					p.replyOnCommit(p.log[m.Slot].command)
				} else {
					p.exec()
				}
			}
		}
	}
}

func (p *FastPaxos) hasConflict(slot int) bool {
	p2votes, p2bsExist := p.p2bCommandVotes[slot]
	if p2bsExist {
		totalVotes := 0
		maxQfAckSize := 0
		for _, p2vote := range p2votes {
			totalVotes += p2vote.Q.Size()
			if maxQfAckSize < p2vote.Q.Size() {
				maxQfAckSize = p2vote.Q.Size()
			}
		}
		n := paxi.GetConfig().N()
		if n - totalVotes < p.Q2fSize - maxQfAckSize {
			log.Debugf("Conflict Detected on slot %d: n: %d, totalVotes: %d, QfSize: %d, maxQfAckSize: %d", slot, n, totalVotes, p.Q2fSize, maxQfAckSize)
			return true // it is no longer possible to form a fast quorum
		} else {
			log.Debugf("NO Conflict Detected on slot %d: n: %d, totalVotes: %d, QfSize: %d, maxQfAckSize: %d", slot, n, totalVotes, p.Q2fSize,  maxQfAckSize)
		}
	}
	return false
}

/**
this function takes p2bs from fast round and treats them as p1bs for classical round for recovery
the output of this function is the command to prepare in classical P2
 */
func (p *FastPaxos) p1bResultFromP2bFast(slot int) *paxi.Command{
	p2votes, p2bsExist := p.p2bCommandVotes[slot]
	if p2bsExist {
		totalVotes := 0
		maxQfAckSize := 0
		var cmd *paxi.Command
		for _, p2vote := range p2votes {
			totalVotes += p2vote.Q.Size()
			if maxQfAckSize < p2vote.Q.Size() {
				maxQfAckSize = p2vote.Q.Size()
				cmd = &p2vote.Cmd
			}
		}
		n := paxi.GetConfig().N()
		log.Debugf("p1bResultFromP2bFast. totalVotes: %d, maxQfAckSize: %d, QcSize: %d", totalVotes, maxQfAckSize, p.Q2cSize)
		if totalVotes >= p.Q2cSize && maxQfAckSize > n - p.Q2fSize {
			// we have at least Qc size of replies, so we can pick a safe value if there is one
			return cmd
		}
	}
	return nil
}

func (p *FastPaxos) replyOnCommit(cmd paxi.Command) {
	log.Debugf("Replica %s replyOnCommit [s=%d, cmd=%v]", p.ID(), cmd)

	p.learnerlog[p.execute] = &paxi.Command{Key: cmd.Key, Value: cmd.Value, CommandID: cmd.CommandID}
	if !p.replyIfPossibleForSlot(p.execute) {
		p.replyIfPossible()
	}

	if p.execute > 100 {
		if _, exists := p.p2bCommandVotes[p.execute - 100]; exists {
			delete(p.p2bCommandVotes, p.execute - 100)
		}
	}
	p.execute++
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
			p.IsFastBallot = true
		}

		p.learnerlog[p.execute] = &paxi.Command{Key: e.command.Key, Value: value, CommandID:e.command.CommandID}
		if !p.replyIfPossibleForSlot(p.execute) {
			p.replyIfPossible()
		}

		// TODO clean up the log periodically instead of using some slack value
		if p.execute > cleanupSlack {
			if _, exists := p.p2bCommandVotes[p.execute - cleanupSlack]; exists {
				delete(p.p2bCommandVotes, p.execute - cleanupSlack)
			}
			delete(p.log, p.execute - cleanupSlack)
		}
		p.execute++
	}
}

func (p *FastPaxos) replyIfPossibleForSlot(slot int) bool {
	p.Lock()
	defer p.Unlock()
	tThreshold := time.Now().UnixNano() - paxi.GetConfig().ClientTimeout * 1000000
	for ts, r := range p.requests {
		if llog, exsts := p.learnerlog[slot]; exsts {
			if r.Command.CommandID == llog.CommandID && r.Command.Key == llog.Key {
				log.Debugf("Replica %s Reply on slot %d from learnedLog command %v\n", p.ID(), slot, llog)
				reply := paxi.Reply{
					Command:    *llog,
					Value:      llog.Value,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
				r.Reply(reply)
				delete(p.learnerlog, slot)
				delete(p.requests, ts)
				return true
			}
		}
		if p.requestTimeout(r, tThreshold) {
			delete(p.requests, ts)
		}
	}
	return false
}

func (p *FastPaxos) replyIfPossible() bool {
	p.Lock()
	defer p.Unlock()
	for ts, r := range p.requests {
		for slot, llog := range p.learnerlog {
			if r.Command.CommandID == llog.CommandID && r.Command.Key == llog.Key {
				log.Debugf("Replica %s Reply on slot from learnedLog command %v\n", p.ID(), slot, llog)
				reply := paxi.Reply{
					Command:    *llog,
					Value:      llog.Value,
					Properties: make(map[string]string),
				}
				reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
				r.Reply(reply)
				delete(p.learnerlog, slot)
				delete(p.requests, ts)
				return true
			}
		}
	}
	return false
}

func (p *FastPaxos) enforceTimeoutOnAllOutstandingRequests() {
	p.Lock()
	defer p.Unlock();
	tThreshold := time.Now().UnixNano() - paxi.GetConfig().ClientTimeout * 1000000
	for ts, r := range p.requests {
		if p.requestTimeout(r, tThreshold) {
			delete(p.requests, ts)
		}
	}
}


/**
 * Sometimes a request from client may never get executed, due to conflict handling. In order to not block the
 * Benchmark, we reply nack to the client to allow it to proceed.
 * TODO: Of course, a better solution would be to do a conflict resolve in a way that does not cause this
 * and implement a proper state machine
 */
func (p *FastPaxos) requestTimeout(r *paxi.Request, refTime int64) bool {
	log.Debugf("Replica %s request cmd %v, with timestamp %d, expire on %d\n", p.ID(), r.Command, r.Timestamp, refTime)
	if r.Timestamp < refTime {
		// we timeout, reply nack to client
		log.Debugf("Replica %s Reply lost slot for command %v\n", p.ID(), r.Command)
		reply := paxi.Reply{
			Command:    r.Command,
			Properties: make(map[string]string),
		}
		reply.Properties[HTTPLostSlot] = "true"
		reply.Properties[HTTPHeaderLeader] = strconv.FormatBool(p.IsLeader())
		r.Reply(reply)
		return true
	}
	return false
}
