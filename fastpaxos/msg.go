package fastpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
	Subballot int
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v, sub=%v}", m.Ballot, m.Subballot)
}

// CommandBallot combines each command with its ballot number
type CommandBallot struct {
	Command   paxi.Command
	Ballot    paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("cmd=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v id=%s log=%v}", m.Ballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	Ballot      paxi.Ballot
	Slot        int
	Command     paxi.Command
	fromClient  bool // non-exported flag set when creating this p2a from a client request
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot        paxi.Ballot
	Command       paxi.Command
	ID            paxi.ID // from node id
	Slot          int
	ClassicQuourm bool // flag set when resolving a slot using a classic quorum
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v id=%s s=%d, cmd=%v, resolve=%t}", m.Ballot, m.ID, m.Slot, m.Command, m.ClassicQuourm)
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v s=%d cmd=%v}", m.Ballot, m.Slot, m.Command)
}
