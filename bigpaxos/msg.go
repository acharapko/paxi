package bigpaxos

import (
	"encoding/gob"
	"fmt"
	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1b{})
	gob.Register(P2b{})
	gob.Register([]P1b{})
	gob.Register([]P2b{})
	gob.Register(P1a{})
	gob.Register(P2a{})
	gob.Register(P3{})
}

// CommandBallot combines each command with its ballot number
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	ID     paxi.ID               // from node id
	Ballot paxi.Ballot
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}",  m.Ballot, m.ID, m.Log)
}

// P2b accepted message
type P2b struct {
	IsFinal bool
	ID      []paxi.ID // from node id
	Ballot  paxi.Ballot
	Slot    int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {isReply=%v, b=%v id=%s s=%d}", m.IsFinal, m.Ballot, m.ID, m.Slot)
}

// P1a prepare message
type P1a struct {
	Depth  uint8
	Source paxi.ID
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {source=%v, depth=%d, b=%v}", m.Source, m.Depth, m.Ballot)
}

// P2a accept message
type P2a struct {
	Depth   uint8
	Source  paxi.ID
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {source=%v, depth=%d, b=%v s=%d cmd=%v}", m.Source, m.Depth, m.Ballot, m.Slot, m.Command)
}


// P3 commit message
type P3 struct {
	Depth   uint8
	Source  paxi.ID
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m *P3) String() string {
	return fmt.Sprintf("P3 {source=%v, depth=%d, b=%v s=%d cmd=%v}", m.Source, m.Depth, m.Ballot, m.Slot, m.Command)
}
