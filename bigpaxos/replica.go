package bigpaxos

import (
	"flag"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"sort"
	"github.com/ailidani/paxi/paxos"
	"time"
)

var ephemeralLeader = flag.Bool("ephemeral", false, "stable leader, if true paxos forward request to current leader")
var pg = flag.Int("pg", 2, "Number of peer-groups. Default is 2")
var stdTimeout = flag.Int("stdtimeout", 50, "Standard timeout after which all non-collected reponses are treated as failures")

type BalSlot struct {
	paxi.Ballot
	slot int
}

type RelayToLeader struct {
	relayTimeInt int64
	BalSlot
}

// Replica for one BigPaxos instance
type Replica struct {
	paxi.Node
	*paxos.Paxos
	peerGroups            [][]paxi.ID
	myPeerGroup           int
	p1bRelaysMapByBallot  map[paxi.Ballot][]paxos.P1b
	p2bRelaysMapByBalSlot map[int]*paxos.P2b
	maxP2bBallot paxi.Ballot

	p1bRelaysListSortedByTime []RelayToLeader
	p2bRelaysListSortedByTime []RelayToLeader
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	log.Debugf("BigPaxos Starting replica %v", id)
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Paxos = paxos.NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Envelope{}, r.handleMsg)
	r.Register(paxos.P1a{}, r.Paxos.HandleP1a)
	r.Register(paxos.P2a{}, r.Paxos.HandleP2a)
	r.Register(paxos.P1b{}, r.handleP1b)
	r.Register([]paxos.P1b{}, r.handleP1bLeader)
	r.Register(paxos.P2b{}, r.handleP2b)
	r.Register([]paxos.P2b{}, r.handleP2bLeader)
	r.Register(paxos.P3{}, r.Paxos.HandleP3)

	r.p1bRelaysMapByBallot = make(map[paxi.Ballot][]paxos.P1b)
	r.p2bRelaysMapByBalSlot = make(map[int]*paxos.P2b)
	r.p1bRelaysListSortedByTime = make([]RelayToLeader, 0)
	r.p2bRelaysListSortedByTime = make([]RelayToLeader, 0)

	r.peerGroups = make([][]paxi.ID, *pg)
	log.Debugf("len(pg) : %d", len(r.peerGroups))
	knownIDs := make([]paxi.ID, 0, len(paxi.GetConfig().Addrs))
	for id := range paxi.GetConfig().Addrs {
		knownIDs = append(knownIDs, id)
	}
	sort.Slice(knownIDs, func(i, j int) bool {
		return knownIDs[i].Zone() < knownIDs[j].Zone() ||
			(knownIDs[i].Zone() == knownIDs[j].Zone() && knownIDs[i].Node() < knownIDs[j].Node())
	})

	log.Debugf("Known IDs : %v", knownIDs)
	nodesPerGroup := len(knownIDs) / *pg
	pgNum := 0
	r.peerGroups[pgNum] = make([]paxi.ID, 0)
	nodesAddToPg := 0
	for _, id := range knownIDs {
		if id == r.ID() {
			r.myPeerGroup = pgNum
		}

		r.peerGroups[pgNum] = append(r.peerGroups[pgNum], id)
		nodesAddToPg++
		if (nodesAddToPg >= nodesPerGroup && pgNum + 1 < *pg) {
			pgNum++
			nodesAddToPg = 0
			r.peerGroups[pgNum] = make([]paxi.ID, 0)
		}
	}
	log.Debugf("BigPaxos computed PeerGroups: {%v}", r.peerGroups)

	return r
}

func (r *Replica) startReplyTicker() {
	go func() {
		for now := range time.Tick(10 * time.Millisecond) {
			now.UnixNano()
		}
	}()
}

func (r *Replica) Broadcast(m interface{}) {
	envelope := Envelope{NestingLayer: 0,  IsReply: false, Payload: m}
	log.Debugf("BigPaxos Broadcast: {%v}", envelope)
	for i := 0; i < *pg; i++{
		for _, id := range r.peerGroups[i] {
			if id != r.ID() {
				log.Debugf("BigPaxos Broadcast. Sending to PeerGroup %d, node %v", i, id)
				r.Send(id, envelope)
				break
			}
		}
	}
}

func (r *Replica) BroadcastToPeerGroup(pgNum int, originalSourceToExclude paxi.ID, m interface{}) {
	log.Debugf("BigPaxos Broadcast to PeerGroup %d: {%v}", pgNum, m)
	for _, id := range r.peerGroups[pgNum] {
		if id != r.ID() && id != originalSourceToExclude {
			log.Debugf("Sending to %v", id)
			r.Send(id, m)
		}
	}
}

func (r *Replica) handleP1b(m paxos.P1b) {
	if m.Ballot.ID() != r.ID() {
		// we are just relaying this message
		log.Debugf("Node %v received P1b for relay {%v}", r.ID(), m)
		if _, ok := r.p1bRelaysMapByBallot[m.Ballot]; !ok {
			log.Debugf("Unknown P1b {%v} Ballot to relay. This ballot may have already been replied", m)
		} else {
			r.p1bRelaysMapByBallot[m.Ballot] = append(r.p1bRelaysMapByBallot[m.Ballot], m)
			log.Debugf("Now have %d messages to relay for p1b Ballot %v", len(r.p1bRelaysMapByBallot[m.Ballot]), m.Ballot)
			if r.readyToRelayP1b(m.Ballot) {
				log.Debugf("Relaying p1bs {%v} to %v", r.p1bRelaysMapByBallot[m.Ballot], m.Ballot.ID())
				replyEnvelop := Envelope{NestingLayer: 0, IsReply: true, Payload: r.p1bRelaysMapByBallot[m.Ballot]}
				r.Send(m.Ballot.ID(), replyEnvelop)
				delete(r.p1bRelaysMapByBallot, m.Ballot)
			}
		}
	} else {
		// we should never be here
		log.Errorf("Node {%v} received P1b with Ballot {%v} for itself! This should not happen in BigPaxos", r.ID(), m.Ballot)
	}
}

func (r *Replica) handleP1bLeader(p1bs []paxos.P1b) {
	for _, p1b := range p1bs {
		r.HandleP1b(p1b)
	}
}

func (r *Replica) handleP2b(m paxos.P2b) {
	if m.Ballot.ID() != r.ID() {
		// we are just relaying this message
		log.Debugf("Node %v received P2b for relay {%v}", r.ID(), m)
		if _, haveSlot := r.p2bRelaysMapByBalSlot[m.Slot]; !haveSlot {
			log.Debugf("Unknown P2b {%v} Ballot to relay. It may have already been replied", m)
		} else {
			if r.p2bRelaysMapByBalSlot[m.Slot].Ballot == m.Ballot {
				r.p2bRelaysMapByBalSlot[m.Slot].ID = append(r.p2bRelaysMapByBalSlot[m.Slot].ID, m.ID...)
				log.Debugf("Now have %d messages to relay for p2b Slot %d Ballot %v", len(r.p2bRelaysMapByBalSlot[m.Slot].ID), m.Slot, m.Ballot)
				if r.readyToRelayP2b(m.Slot) {
					log.Debugf("Relaying p2bs {%v} to %v", r.p2bRelaysMapByBalSlot[m.Slot], m.Ballot.ID())
					replyEnvelop := Envelope{NestingLayer: 0, IsReply: true, Payload: r.p2bRelaysMapByBalSlot[m.Slot]}
					r.Send(m.Ballot.ID(), replyEnvelop)
					delete(r.p2bRelaysMapByBalSlot, m.Slot)
				}
			} else {
				log.Errorf("Node %v received P2b to rely with non-matching ballot (%v) to the relay ballot (%v)",
					m.Ballot, r.p2bRelaysMapByBalSlot[m.Slot].Ballot)
			}
		}
	} else {
		// we should never be here
		log.Errorf("Node {%v} received P1b with Ballot {%v} for itself! This should not happen in BigPaxos", r.ID(), m.Ballot)
	}
}

func (r *Replica) handleP2bLeader(p2bs []paxos.P2b) {
	for _, p2b := range p2bs {
		r.HandleP2b(p2b)
	}
}


func (r *Replica) readyToRelayP1b(ballot paxi.Ballot) bool {
	if len(r.p1bRelaysMapByBallot[ballot]) == len(r.peerGroups[r.myPeerGroup]) - 1 {
		return true
	}
	if len(r.p1bRelaysMapByBallot[ballot]) == len(r.peerGroups[r.myPeerGroup]) - 2 {
		for _, id := range r.peerGroups[r.myPeerGroup] {
			if id == ballot.ID() {
				return true
			}
		}
	}
	return false
}

func (r *Replica) readyToRelayP2b(m int) bool {
	if len(r.p2bRelaysMapByBalSlot[m].ID) == len(r.peerGroups[r.myPeerGroup]) - 1 {
		return true
	}
	if len(r.p2bRelaysMapByBalSlot[m].ID) == len(r.peerGroups[r.myPeerGroup]) - 2 {
		for _, id := range r.peerGroups[r.myPeerGroup] {
			if id == r.p2bRelaysMapByBalSlot[m].Ballot.ID() {
				return true
			}
		}
	}
	return false
}

func (r *Replica) handleMsg(m Envelope) {
	log.Debugf("BigPaxos Handle Msg: {%v}", m)
	if m.IsReply {
		// aggregating replies from all nodes at leader
		r.HandleMsg(m.Payload)
	} else {
		// propagating messages from leader to followers.
		r.forwardPropagation(m)
	}
}


func (r *Replica) forwardPropagation(m Envelope) {
	if m.NestingLayer == 0 {
		// we need to resend message to layer 1 and wait for replies or timeout
		var originalSource paxi.ID
		if p1a, ok := m.Payload.(paxos.P1a); ok {
			originalSource = p1a.Source
			p1a.Source = r.ID()
			m.Payload = p1a
			if _, ok := r.p1bRelaysMapByBallot[p1a.Ballot]; !ok {
				r.p1bRelaysMapByBallot[p1a.Ballot] = make([]paxos.P1b, 0, len(r.peerGroups[r.myPeerGroup]))
				//TODO: add timer send action for all relays
			}
		} else if p2a, ok := m.Payload.(paxos.P2a); ok {
			originalSource = p2a.Source
			p2a.Source = r.ID()
			m.Payload = p2a
			if _, ok := r.p2bRelaysMapByBalSlot[p2a.Slot]; !ok {
				r.p2bRelaysMapByBalSlot[p2a.Slot] = &paxos.P2b{Ballot: p2a.Ballot, Slot: p2a.Slot, ID: make([]paxi.ID, 0)}

				//TODO: add timer send action for all relays
			} else {
				// we have this slot already. Check ballot. if we were collecting responses for lesser ballot,
				// we can reply nack to old sender
				// if we were collecting responses for higher ballot reply nack to new sender
				if r.p2bRelaysMapByBalSlot[p2a.Slot].Ballot < p2a.Ballot {
					nackP2b := &paxos.P2b{Ballot: p2a.Ballot, Slot: p2a.Slot, ID: make([]paxi.ID, 0)}
					replyEnvelop := Envelope{NestingLayer: 0, IsReply: true, Payload: nackP2b}
					r.Send(r.p2bRelaysMapByBalSlot[p2a.Slot].Ballot.ID(), replyEnvelop)
					r.p2bRelaysMapByBalSlot[p2a.Slot] = &paxos.P2b{Ballot: p2a.Ballot, Slot: p2a.Slot, ID: make([]paxi.ID, 0)}
					return
				} else if r.p2bRelaysMapByBalSlot[p2a.Slot].Ballot > p2a.Ballot {
					nackP2b := &paxos.P2b{Ballot: r.p2bRelaysMapByBalSlot[p2a.Slot].Ballot, Slot: p2a.Slot, ID: make([]paxi.ID, 0)}
					replyEnvelop := Envelope{NestingLayer: 0, IsReply: true, Payload: nackP2b}
					r.Send(r.p2bRelaysMapByBalSlot[p2a.Slot].Ballot.ID(), replyEnvelop)
					return
				}
			}
		} else if p3, ok := m.Payload.(paxos.P3); ok {
			originalSource = p3.Source
			p3.Source = r.ID()
			m.Payload = p3
		}

		m.NestingLayer++
		r.BroadcastToPeerGroup(r.myPeerGroup, originalSource, m)

	} else {
		// we are in the terminal layer, so just reply when processed
		log.Debugf("Node %v handling msg {%v}", r.ID(), m)
		r.HandleMsg(m.Payload)
	}
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if *ephemeralLeader || r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
		r.Paxos.HandleRequest(m)
	} else {
		go r.Forward(r.Paxos.Leader(), m)
	}
}
