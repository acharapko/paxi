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

// CommandBallot conbines each command with its ballot number
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
	Subballot int
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v subb=%v id=%s log=%v}", m.Ballot, m.Subballot, m.ID, m.Log)
}

// P2a accept message
type P2a struct {
	Ballot      paxi.Ballot
	ConflictNum int
	Slot        int
	Command     paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v subb=%v s=%d cmd=%v}", m.Ballot, m.ConflictNum, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot     paxi.Ballot
	ConflicNum int
	Command    paxi.Command
	ID         paxi.ID // from node id
	Slot       int
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v subb=%v id=%s s=%d, cmd=%v}", m.Ballot, m.ConflicNum, m.ID, m.Slot, m.Command)
}

// P3 commit message
type P3 struct {
	Ballot  paxi.Ballot
	Subballot int
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%v subb=%v s=%d cmd=%v}", m.Ballot, m.Subballot, m.Slot, m.Command)
}
