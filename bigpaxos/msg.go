package bigpaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi/paxos"
)

func init() {
	gob.Register(Envelope{})
	gob.Register(paxos.P1a{})
	gob.Register([]paxos.P1b{})
	gob.Register([]paxos.P2b{})
}

type Envelope struct {
	NestingLayer int8
	IsReply bool
	Payload interface{}
}

func (m Envelope) String() string {
	return fmt.Sprintf("Envelope {NestingLayer %d, IsReply: %v, payload=%v}", m.NestingLayer, m.IsReply, m.Payload)
}
