package bigpaxos

import (
	"time"

	"github.com/ailidani/paxi"
	"sync"
	"github.com/ailidani/paxi/log"
)

// entry in log
type entry struct {
	ballot    paxi.Ballot
	command   paxi.Command
	commit    bool
	request   *paxi.Request
	quorum    *paxi.Quorum
	timestamp time.Time
}

// Paxos instance
type BigPaxos struct {
	paxi.Node

	config []paxi.ID

	log     map[int]*entry // log ordered by slot
	execute int            // next execute slot number
	active  bool           // active leader
	ballot  paxi.Ballot    // highest ballot number
	slot    int            // highest slot number
	p1aTime int64		   // time P1a was sent

	quorum   *paxi.Quorum    // phase 1 quorum
	requests []*paxi.Request // phase 1 pending requests

	Q1              func(*paxi.Quorum) bool
	Q2              func(*paxi.Quorum) bool
	ReplyWhenCommit bool

	logLck	sync.RWMutex
}

// NewPaxos creates new paxos instance
func NewBigPaxos(n paxi.Node, options ...func(*BigPaxos)) *BigPaxos {
	p := &BigPaxos{
		Node:            n,
		log:             make(map[int]*entry, paxi.GetConfig().BufferSize),
		slot:            -1,
		quorum:          paxi.NewQuorum(),
		requests:        make([]*paxi.Request, 0),
		Q1:              func(q *paxi.Quorum) bool { return q.Majority() },
		Q2:              func(q *paxi.Quorum) bool { return q.Majority() },
		ReplyWhenCommit: false,
	}

	for _, opt := range options {
		opt(p)
	}

	return p
}

// IsLeader indecates if this node is current leader
func (p *BigPaxos) IsLeader() bool {
	return p.active || p.ballot.ID() == p.ID()
}

func (p *BigPaxos) CheckTimeout(timeout int64) {
	p.logLck.RLock()
	defer p.logLck.RUnlock()
	execslot := p.execute
	if p.active && execslot < p.slot {
		if entry, ok := p.log[execslot]; ok {
			if entry.timestamp.UnixNano() < timeout {
				p.RetryP2a(execslot)
			}
		}
	} else if !p.active && p.p1aTime < timeout {
		p.RetryP1a()
	}
}

// Leader returns leader id of the current ballot
func (p *BigPaxos) Leader() paxi.ID {
	return p.ballot.ID()
}

// Ballot returns current ballot
func (p *BigPaxos) Ballot() paxi.Ballot {
	return p.ballot
}

// SetActive sets current paxos instance as active leader
func (p *BigPaxos) SetActive(active bool) {
	p.active = active
}

// SetBallot sets a new ballot number
func (p *BigPaxos) SetBallot(b paxi.Ballot) {
	p.ballot = b
}

// HandleRequest handles request and start phase 1 or phase 2
func (p *BigPaxos) HandleRequest(r paxi.Request) {
	// log.Debugf("Replica %s received %v\n", p.ID(), r)
	if !p.active {
		p.requests = append(p.requests, &r)
		// current phase 1 pending
		if p.ballot.ID() != p.ID() {
			p.P1a()
		}
	} else {
		p.P2a(&r)
	}
}

// P1a starts phase 1 prepare
func (p *BigPaxos) P1a() {
	if p.active {
		return
	}
	p.ballot.Next(p.ID())
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.p1aTime = time.Now().UnixNano()
	p.Broadcast(P1a{Ballot: p.ballot})
}

func (p *BigPaxos) RetryP1a() {
	if p.active {
		return
	}
	p.quorum.Reset()
	p.quorum.ACK(p.ID())
	p.p1aTime = time.Now().UnixNano()
	p.Broadcast(P1a{Ballot: p.ballot})
}

// P2a starts phase 2 accept
func (p *BigPaxos) P2a(r *paxi.Request) {
	p.slot++
	log.Debugf("Entering P2a with slot %d", p.slot)
	p.logLck.Lock()
	p.log[p.slot] = &entry{
		ballot:    p.ballot,
		command:   r.Command,
		request:   r,
		quorum:    paxi.NewQuorum(),
		timestamp: time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID())
	p.logLck.Unlock()
	m := P2a{
		Ballot:  p.ballot,
		Slot:    p.slot,
		Command: r.Command,
	}
	if paxi.GetConfig().Thrifty {
		p.Broadcast(m) // TODO: implement thrifty
	} else {
		p.Broadcast(m)
	}
	log.Debugf("Leaving P2a with slot %d", p.slot)
}

func (p *BigPaxos) RetryP2a(slot int) {
	p.logLck.RLock()
	entry, ok := p.log[slot]
	p.logLck.RUnlock()
	if ok {
		m := P2a{
			Ballot:  p.ballot,
			Slot:    p.slot,
			Command: entry.command,
		}
		if paxi.GetConfig().Thrifty {
			p.Broadcast(m) // TODO: implement thrifty
		} else {
			p.Broadcast(m)
		}
	}
}

// HandleP1a handles P1a message
func (p *BigPaxos) HandleP1a(m P1a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	// new leader
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
		p.forward()
	}

	l := make(map[int]CommandBallot)
	p.logLck.RLock()
	for s := p.execute; s <= p.slot; s++ {
		if p.log[s] == nil || p.log[s].commit {
			continue
		}
		l[s] = CommandBallot{p.log[s].command, p.log[s].ballot}
	}
	p.logLck.RUnlock()

	p.Send(m.Source, P1b{
		Ballot: p.ballot,
		ID:     p.ID(),
		Log:    l,
	})
	log.Debugf("Leaving HandleP1a")
}

func (p *BigPaxos) update(scb map[int]CommandBallot) {
	p.logLck.Lock()
	defer p.logLck.Unlock()
	for s, cb := range scb {
		p.slot = paxi.Max(p.slot, s)
		if e, exists := p.log[s]; exists {
			if !e.commit && cb.Ballot > e.ballot {
				e.ballot = cb.Ballot
				e.command = cb.Command
			}
		} else {
			p.log[s] = &entry{
				ballot:  cb.Ballot,
				command: cb.Command,
				commit:  false,
			}
		}
	}
}

// HandleP1b handles P1b message
func (p *BigPaxos) HandleP1b(m P1b) {
	// old message
	if m.Ballot < p.ballot || p.active {
		return
	}

	p.update(m.Log)

	// reject message
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false // not necessary
		// forward pending requests to new leader
		p.forward()
		// p.P1a()
	}

	// ack message
	if m.Ballot.ID() == p.ID() && m.Ballot == p.ballot {
		p.quorum.ACK(m.ID)
		if p.Q1(p.quorum) {
			p.active = true
			// propose any uncommitted entries
			p.logLck.Lock()
			for i := p.execute; i <= p.slot; i++ {
				// TODO nil gap?
				if p.log[i] == nil || p.log[i].commit {
					continue
				}
				p.log[i].ballot = p.ballot
				p.log[i].quorum = paxi.NewQuorum()
				p.log[i].quorum.ACK(p.ID())
				p.Broadcast(P2a{
					Ballot:  p.ballot,
					Slot:    i,
					Command: p.log[i].command,
				})
			}
			p.logLck.Unlock()
			// propose new commands
			for _, req := range p.requests {
				p.P2a(req)
			}
			p.requests = make([]*paxi.Request, 0)
		}
	}
}

// HandleP2a handles P2a message
func (p *BigPaxos) HandleP2a(m P2a) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	if m.Ballot >= p.ballot {
		p.ballot = m.Ballot
		p.active = false
		// update slot number
		p.slot = paxi.Max(p.slot, m.Slot)
		// update entry
		p.logLck.Lock()
		if e, exists := p.log[m.Slot]; exists {
			if !e.commit && m.Ballot > e.ballot {
				// different command and request is not nil
				if !e.command.Equal(m.Command) && e.request != nil {
					p.Forward(m.Ballot.ID(), *e.request)
					// p.Retry(*e.request)
					e.request = nil
				}
				e.command = m.Command
				e.ballot = m.Ballot
			}
		} else {
			p.log[m.Slot] = &entry{
				ballot:  m.Ballot,
				command: m.Command,
				commit:  false,
			}
		}
		p.logLck.Unlock()
	}

	idList := make([]paxi.ID, 1, 1)
	idList[0] = p.ID()
	p.Send(m.Source, P2b{
		Ballot: p.ballot,
		Slot:   m.Slot,
		ID:     idList,
	})
	log.Debugf("Leaving HandleP2a")
}

// HandleP2b handles P2b message
func (p *BigPaxos) HandleP2b(m P2b) {
	log.Debugf("Entering HandleP1a: %s ===[%v]===>>> %s", m.Ballot.ID(), p.ID())
	// old message
	p.logLck.RLock()
	entry, exist := p.log[m.Slot]
	p.logLck.RUnlock()
	if !exist || m.Ballot < entry.ballot || entry.commit {
		return
	}
	// reject message
	// node update its ballot number and falls back to acceptor
	if m.Ballot > p.ballot {
		p.ballot = m.Ballot
		p.active = false
	}

	// ack message
	// the current slot might still be committed with q2
	// if no q2 can be formed, this slot will be retried when received p2a or p3
	if m.Ballot.ID() == p.ID() && m.Ballot == entry.ballot {
		for _, id := range m.ID {
			entry.quorum.ACK(id)
		}

		if p.Q2(entry.quorum) {
			entry.commit = true
			p.Broadcast(P3{
				Ballot:  m.Ballot,
				Slot:    m.Slot,
				Command: entry.command,
			})

			if p.ReplyWhenCommit {
				r := entry.request
				r.Reply(paxi.Reply{
					Command:   r.Command,
					Timestamp: r.Timestamp,
				})
			} else {
				p.exec()
			}
		}
	}
	log.Debugf("Leaving HandleP2b")
}

// HandleP3 handles phase 3 commit message
func (p *BigPaxos) HandleP3(m P3) {
	log.Debugf("Replica %s ===[%v]===>>> Replica %s\n", m.Ballot.ID(), m, p.ID())

	p.slot = paxi.Max(p.slot, m.Slot)
	p.logLck.Lock()
	e, exist := p.log[m.Slot]
	if exist {
		if !e.command.Equal(m.Command) && e.request != nil {
			// p.Retry(*e.request)
			p.Forward(m.Ballot.ID(), *e.request)
			e.request = nil
		}
	} else {
		p.log[m.Slot] = &entry{}
		e = p.log[m.Slot]
	}
	p.logLck.Unlock()

	e.command = m.Command
	e.commit = true

	if p.ReplyWhenCommit {
		if e.request != nil {
			e.request.Reply(paxi.Reply{
				Command:   e.request.Command,
				Timestamp: e.request.Timestamp,
			})
		}
	} else {
		p.exec()
	}
	log.Debugf("Leaving HandleP3")
}

func (p *BigPaxos) exec() {
	log.Debugf("Entering exec. exec slot=%d", p.execute)
	p.logLck.Lock()
	defer p.logLck.Unlock()
	for {
		e, ok := p.log[p.execute]
		if !ok || !e.commit {
			break
		}
		// log.Debugf("Replica %s execute [s=%d, cmd=%v]", p.ID(), p.execute, e.command)
		value := p.Execute(e.command)
		if e.request != nil {
			reply := paxi.Reply{
				Command:    e.command,
				Value:      value,
				Properties: make(map[string]string),
			}
			e.request.Reply(reply)
			e.request = nil
		}
		// TODO clean up the log periodically
		delete(p.log, p.execute)
		p.execute++
	}
	log.Debugf("Leaving exec")
}

func (p *BigPaxos) forward() {
	for _, m := range p.requests {
		p.Forward(p.ballot.ID(), *m)
	}
	p.requests = make([]*paxi.Request, 0)
}
