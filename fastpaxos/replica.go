package fastpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"flag"
)

var p1q = flag.Int("p1q", 0, "phase-1 quorum size")
var p2qf = flag.Int("p2qf", 0, "phase-2-fast quorum size")
var p2qc = flag.Int("p2qc", 0, "phase-2-classical quorum size")
var reelectiontime = flag.Int("reelectiontime", 0, "How often to re-elect a leader. 0 means do not re-elect")

const (
	HTTPHeaderLeader    = "Leader"
	HTTPLostSlot    = "Lostslot"
)

// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*FastPaxos
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.FastPaxos = NewPaxos(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(P1a{}, r.HandleP1a)
	r.Register(P1b{}, r.HandleP1b)
	r.Register(P2a{}, r.HandleP2aFollower)
	r.Register(P2b{}, r.HandleP2b)
	r.Register(P3{}, r.HandleP3)
	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	r.FastPaxos.HandleRequest(m)
}
