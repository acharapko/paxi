package bigpaxos

import (
	"flag"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"sort"
	"time"
	"math/rand"
	"sync"
)

var ephemeralLeader = flag.Bool("ephemeral", false, "stable leader, if true paxos forward request to current leader")
var pg = flag.Int("pg", 2, "Number of peer-groups. Default is 2")
var regionPeerGroups = flag.Bool("rpg", false, "use region as a peer group instead")
var stdTimeout = flag.Int("stdtimeout", 50, "Standard timeout after which all non-collected responses are treated as failures")

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
	*BigPaxos
	peerGroups            		[][]paxi.ID
	myPeerGroup           		int
	numPeerGroups				int
	p1bRelays			  		[]P1b
	pendingP1bRelay		  		int64
	p2bRelaysMapByBalSlot 		map[int]*P2b
	p2bRelaysTimeMapByBalSlot 	map[int]int64

	sync.RWMutex
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	log.Debugf("BigPaxos Starting replica %v", id)
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.BigPaxos = NewBigPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.BigPaxos.HandleP1a)
	r.Register(P2a{}, r.BigPaxos.HandleP2a)
	r.Register(P1b{}, r.handleP1b)
	r.Register([]P1b{}, r.handleP1bLeader)
	r.Register(P2b{}, r.handleP2b)
	r.Register(P3{}, r.handleP3)
	r.Register(P1a{}, r.handleP1a)
	r.Register(P2a{}, r.handleP2a)

	r.pendingP1bRelay = 0
	r.p1bRelays = make([]P1b, 0)
	r.p2bRelaysMapByBalSlot = make(map[int]*P2b)
	r.p2bRelaysTimeMapByBalSlot = make(map[int]int64)

	knownIDs := make([]paxi.ID, 0, len(paxi.GetConfig().Addrs))
	for id := range paxi.GetConfig().Addrs {
		knownIDs = append(knownIDs, id)
	}

	sort.Slice(knownIDs, func(i, j int) bool {
		return knownIDs[i].Zone() < knownIDs[j].Zone() ||
			(knownIDs[i].Zone() == knownIDs[j].Zone() && knownIDs[i].Node() < knownIDs[j].Node())
	})

	log.Debugf("Known IDs : %v", knownIDs)

	if !*regionPeerGroups {
		r.numPeerGroups = *pg;
		r.peerGroups = make([][]paxi.ID, r.numPeerGroups)
		log.Debugf("len(pg) : %d", len(r.peerGroups))
		nodesPerGroup := len(knownIDs) / r.numPeerGroups
		pgNum := 0
		r.peerGroups[pgNum] = make([]paxi.ID, 0)
		nodesAddToPg := 0
		for _, id := range knownIDs {
			if id == r.ID() {
				r.myPeerGroup = pgNum
			}

			r.peerGroups[pgNum] = append(r.peerGroups[pgNum], id)
			nodesAddToPg++
			if (nodesAddToPg >= nodesPerGroup && pgNum+1 < r.numPeerGroups) {
				pgNum++
				nodesAddToPg = 0
				r.peerGroups[pgNum] = make([]paxi.ID, 0)
			}
		}

		log.Debugf("BigPaxos computed PeerGroups: {%v}", r.peerGroups)
	} else {
		r.numPeerGroups = paxi.GetConfig().Z()
		r.peerGroups = make([][]paxi.ID, r.numPeerGroups)
		r.myPeerGroup = r.ID().Zone() - 1
		for _, id := range knownIDs {
			pgNum := id.Zone() - 1
			if r.peerGroups[pgNum] == nil {
				r.peerGroups[pgNum] = make([]paxi.ID, 0)
			}

			r.peerGroups[pgNum] = append(r.peerGroups[pgNum], id)
		}

		log.Debugf("BigPaxos computed PeerGroups: {%v}", r.peerGroups)
	}

	go r.startTicker()

	return r
}

func (r *Replica) startTicker() {
	for now := range time.Tick(10 * time.Millisecond) {
		timeoutCutoffTime := now.Add(-time.Duration(*stdTimeout) * time.Millisecond).UnixNano() // everything older than this needs to timeout
		//log.Debugf("Start TimeoutChecker (timeout_cutoff = %d)", timeoutCutoffTime)
		if r.IsLeader() {
			r.CheckTimeout(timeoutCutoffTime)
		} else {
			// check for P1b timeouts
			r.Lock()
			if r.pendingP1bRelay > 0 && r.pendingP1bRelay < timeoutCutoffTime && len(r.p1bRelays) > 0 {
				// we have timeout on P1b
				log.Debugf("Timeout on P1b. Relaying p1bs {%v} to %v", r.p1bRelays, r.p1bRelays[0].Ballot.ID())
				r.Send(r.p1bRelays[0].Ballot.ID(), r.p1bRelays)
				r.p1bRelays = make([]P1b, 0)
				r.pendingP1bRelay = 0
			}
			// check for p2b timeouts
			for slot, p2b := range r.p2bRelaysMapByBalSlot {
				if r.p2bRelaysTimeMapByBalSlot[slot] < timeoutCutoffTime {
					p2b.IsFinal = true;
					log.Debugf("Timeout on P2b. Relaying p2bs {%v} to %v", r.p2bRelaysMapByBalSlot[slot], p2b.Ballot.ID())
					r.Send(p2b.Ballot.ID(), p2b)
					delete(r.p2bRelaysMapByBalSlot, slot)
					delete(r.p2bRelaysTimeMapByBalSlot, slot)
				}
			}
			r.Unlock()
		}
	}
}

//*********************************************************************************************************************
// Messaging
//********************************************************************************************************************

// Overrides Broadcast in node
func (r *Replica) Broadcast(m interface{}) {
	log.Debugf("BigPaxos Broadcast Msg: {%v}", m)
	switch m := m.(type) {
	case P1a:
		m.Source = r.ID()
		m.Depth = 0
		for i := 0; i < r.numPeerGroups; i++ {
			randId := r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			for randId == r.ID() {
				randId = r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			}
			r.Send(randId, m)
		}
	case P2a:
		m.Source = r.ID()
		m.Depth = 0
		for i := 0; i < r.numPeerGroups; i++ {
			randId := r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			for randId == r.ID() {
				randId = r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			}
			r.Send(randId, m)
		}
	case P3:
		m.Source = r.ID()
		m.Depth = 0
		for i := 0; i < r.numPeerGroups; i++ {
			randId := r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			for randId == r.ID() {
				randId = r.peerGroups[i][rand.Intn(len(r.peerGroups[i]))]
			}
			r.Send(randId, m)
		}

	default:
		log.Errorf("We can only broadcast forward messages in BigPaxos. Trying to broadcast {%v}", m)
	}
}

// special broadcast for messages within the peer group
func (r *Replica) BroadcastToPeerGroup(pgNum int, originalSourceToExclude paxi.ID, m interface{}) {
	log.Debugf("BigPaxos Broadcast to PeerGroup %d: {%v}", pgNum, m)
	for _, id := range r.peerGroups[pgNum] {
		if id != r.ID() && id != originalSourceToExclude {
			log.Debugf("Sending to %v", id)
			r.Send(id, m)
		}
	}
}

func (r *Replica) Send(to paxi.ID, m interface{}) {
	if to == r.ID() {
		r.HandleMsg(m) // loopback for self
	} else {
		r.Node.Send(to, m)
	}
}

//*********************************************************************************************************************
// Forward Propagation
//*********************************************************************************************************************

func (r *Replica) handleP1a(m P1a) {
	log.Debugf("Node %v handling msg {%v}", r.ID(), m)
	oldBallot := r.Ballot()
	if m.Depth == 0 {
		originalSource := m.Source
		m.Source = r.ID()
		m.Depth++
		// handle message and reply to self for vore aggreagtion
		r.HandleP1a(m)
		if oldBallot < m.Ballot {
			r.Lock()
			defer r.Unlock()
			if (r.pendingP1bRelay > 0) {
				// this is a ballot we have not seen... and have not relayed before
				// so we can reply nack to any outstanding p1a relays
				log.Debugf("Short circuiting p1a relay. previous ballot=%v, new ballot=%v", oldBallot, m.Ballot)
				r.Send(oldBallot.ID(), m)
			}

			r.pendingP1bRelay = time.Now().UnixNano()
			r.BroadcastToPeerGroup(r.myPeerGroup, originalSource, m)
		} else {
			log.Debugf("Node %v received P1a with ballot %v, however, a newer ballot %v is known", r.ID(), m.Ballot, oldBallot)
		}
	} else {
		// terminal layer. Just handle messages and reply back
		r.HandleP1a(m)
	}
}

func (r *Replica) handleP2a(m P2a) {
	log.Debugf("Node %v handling msg {%v}", r.ID(), m)
	if m.Depth == 0 {
		r.Lock()
		if _, ok := r.p2bRelaysMapByBalSlot[m.Slot]; !ok {
			r.p2bRelaysMapByBalSlot[m.Slot] = &P2b{Ballot: m.Ballot, Slot: m.Slot, ID: make([]paxi.ID, 0)}
			r.p2bRelaysTimeMapByBalSlot[m.Slot] = time.Now().UnixNano()
		} else {
			// we have this slot already. Check ballot. if we were collecting responses for lesser ballot,
			// we can reply nack to old sender with such lesser ballot.
			// if we were collecting responses for higher ballot reply nack to new sender
			if r.p2bRelaysMapByBalSlot[m.Slot].Ballot < m.Ballot {
				nackP2b := &P2b{Ballot: m.Ballot, Slot: m.Slot, ID: make([]paxi.ID, 0)}
				r.Send(r.p2bRelaysMapByBalSlot[m.Slot].Ballot.ID(), nackP2b)
				r.p2bRelaysMapByBalSlot[m.Slot] = &P2b{Ballot: m.Ballot, Slot: m.Slot, ID: make([]paxi.ID, 0)}
				r.p2bRelaysTimeMapByBalSlot[m.Slot] = time.Now().UnixNano()
				return
			} else if r.p2bRelaysMapByBalSlot[m.Slot].Ballot > m.Ballot {
				nackP2b := &P2b{Ballot: r.p2bRelaysMapByBalSlot[m.Slot].Ballot, Slot: m.Slot, ID: make([]paxi.ID, 0)}
				r.Send(r.p2bRelaysMapByBalSlot[m.Slot].Ballot.ID(), nackP2b)
				return
			}
		}
		r.Unlock()
		originalSource := m.Source
		m.Source = r.ID()
		m.Depth++
		r.HandleP2a(m)
		r.BroadcastToPeerGroup(r.myPeerGroup, originalSource, m)
	} else {
		// terminal layer. Just handle messages and reply back
		r.HandleP2a(m)
	}
}

func (r *Replica) handleP3(p3 P3) {
	log.Debugf("Node %v handling msg {%v}", r.ID(), p3)
	r.HandleP3(p3)

	if p3.Depth == 0 {
		originalSource := p3.Source
		p3.Source = r.ID()
		p3.Depth++
		r.BroadcastToPeerGroup(r.myPeerGroup, originalSource, p3)
	}
}

//*********************************************************************************************************************
// Reply Propagation
//*********************************************************************************************************************

func (r *Replica) handleP1b(m P1b) {
	r.Lock()
	defer r.Unlock()
	// we are just relaying this message
	log.Debugf("Node %v received P1b for relay {%v}", r.ID(), m)
	r.p1bRelays = append(r.p1bRelays, m)
	log.Debugf("Now have %d messages to relay for p1b Ballot %v", len(r.p1bRelays), m.Ballot)
	if r.readyToRelayP1b(m.Ballot) {
		log.Debugf("Relaying p1bs {%v} to %v", r.p1bRelays, m.Ballot.ID())
		r.Send(m.Ballot.ID(), r.p1bRelays)
		r.p1bRelays = make([]P1b, 0)
		r.pendingP1bRelay = 0
	}

}

func (r *Replica) handleP1bLeader(p1bs []P1b) {
	log.Debugf("Node %v received aggregated P1b {%v}", r.ID(), p1bs)
	for _, p1b := range p1bs {
		r.HandleP1b(p1b)
	}
}

func (r *Replica) readyToRelayP1b(ballot paxi.Ballot) bool {
	if len(r.p1bRelays) == len(r.peerGroups[r.myPeerGroup]){
		return true
	}
	if len(r.p1bRelays) == len(r.peerGroups[r.myPeerGroup]) - 1 {
		for _, id := range r.peerGroups[r.myPeerGroup] {
			if id == ballot.ID() {
				return true
			}
		}
	}
	return false
}

func (r *Replica) handleP2b(m P2b) {
	if m.IsFinal {
		// we received p2b aggregated reply, so just handle it at the bigpaxos level
		r.HandleP2b(m)
	} else {
		// we are just relaying this message
		r.Lock()
		p2bforRealy, haveSlot := r.p2bRelaysMapByBalSlot[m.Slot];
		r.Unlock()
		log.Debugf("Node %v received P2b for relay {%v}", r.ID(), m)
		if !haveSlot {
			log.Debugf("Unknown P2b {%v} Ballot to relay. It may have already been replied", m)
		} else {
			if p2bforRealy.Ballot == m.Ballot {
				p2bforRealy.ID = append(p2bforRealy.ID, m.ID...)
				log.Debugf("Now have %d messages to relay for p2b Slot %d Ballot %v", len(p2bforRealy.ID), m.Slot, m.Ballot)
				if r.readyToRelayP2b(m.Slot) {
					p2bforRealy.IsFinal = true;
					log.Debugf("Relaying p2bs {%v} to %v", p2bforRealy, m.Ballot.ID())
					r.Send(m.Ballot.ID(), p2bforRealy)
					r.Lock()
					delete(r.p2bRelaysMapByBalSlot, m.Slot)
					delete(r.p2bRelaysTimeMapByBalSlot, m.Slot)
					r.Unlock()
				}
			} else {
				log.Errorf("Node %v received P2b to rely with non-matching ballot (%v) to the relay ballot (%v)",
					m.Ballot, p2bforRealy.Ballot)
			}
		}
	}
}

func (r *Replica) readyToRelayP2b(m int) bool {
	r.RLock()
	defer r.RUnlock()
	if r.p2bRelaysMapByBalSlot[m] == nil {
		return false
	}
	if len(r.p2bRelaysMapByBalSlot[m].ID) == len(r.peerGroups[r.myPeerGroup])  {
		return true
	}
	if len(r.p2bRelaysMapByBalSlot[m].ID) == len(r.peerGroups[r.myPeerGroup]) - 1 {
		for _, id := range r.peerGroups[r.myPeerGroup] {
			if id == r.p2bRelaysMapByBalSlot[m].Ballot.ID() {
				return true
			}
		}
	}
	return false
}


//*********************************************************************************************************************
// Client Request Handling
//*********************************************************************************************************************

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if *ephemeralLeader || r.BigPaxos.IsLeader() || r.BigPaxos.Ballot() == 0 {
		r.BigPaxos.HandleRequest(m)
	} else {
		go r.Forward(r.BigPaxos.Leader(), m)
	}
}
