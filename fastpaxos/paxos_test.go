package fastpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFastPaxos_IsFastQuorumPossible(t *testing.T) {
	p := FastPaxos{nodeCount: 9, Q2fSize: 7}
	p.pendingCmds = NewCommandSet(paxi.GetConfig().BufferSize, false, true)
	numSlots := 3
	p.resolutionLog = make(map[int]*CommandSet, numSlots)
	for slot := 1; slot <= numSlots; slot++ {
		p.resolutionLog[slot] = NewCommandSet(10, true, false)
	}
	v := []paxi.Value{paxi.GenerateRandVal(8), paxi.GenerateRandVal(8), paxi.GenerateRandVal(8)}

	slotOrder := []int{      1,     1,     1,     1,     1,     2,     2,     3,     1,     1,     2,     1,     2,     1,     2,     2,     3,     2,     3}
	nodeOrder := []paxi.ID{"1.1", "1.3", "1.9", "1.6", "1.7", "1.3", "1.6", "1.6", "1.4", "1.8", "1.7", "1.2", "1.1", "1.5", "1.2", "1.5", "1.2", "1.9", "1.9"}
	cmdOrder := []int{       1,     1,     2,     1,     3,     2,     3,     2,     1,     1,     2,     1,     2,     1,     2,     3,     3,     1,     3}
	expQPossible := []bool{true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true, true,   false,  true, false, false}

	// (1) vote Cmd 1 on slot 1 from node 1.1
	for i, slot := range slotOrder {
		log.Debugf("Adding cmd %d to slot %d from node %v", cmdOrder[i], slotOrder[i], nodeOrder[i])
		cmd := &paxi.Command{Key: paxi.Key(cmdOrder[i]), Value: v[cmdOrder[i] - 1], ClientID: "1.1", CommandID: cmdOrder[i]}
		p.resolutionLog[slot].AddWithVote(cmd, nodeOrder[i])
		p.pendingCmds.Add(cmd)
		p.pendingCmds.AddSlotQuorumIfDoesNotExist(cmd, slot, p.resolutionLog[slot].GetQuorum(UniqueCommandIdFromCommand(cmd)))
		ifqp := p.isFastQuorumPossible(slotOrder[i])
		log.Debugf("Can form fast quorum in slot = %t (expected %t)", ifqp, expQPossible[i])
		assert.Equal(t, expQPossible[i], ifqp)
	}
}

func TestFastPaxos_HandleP2b(t *testing.T) {
	paxi.MakeDefaultConfig()
	node := paxi.NewNode("1.1")
	p := NewPaxos(node)
	p.nodeCount = 9
	p.active = true
	p.active = true
	p.pendingCmds = NewCommandSet(paxi.GetConfig().BufferSize, true, true)
	p.decidedCmds = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
	p.resolvingCmds = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
	p.pendingReply = NewCommandSet(paxi.GetConfig().BufferSize, false, false)
	p.conflictSlots = make(map[int]bool, paxi.GetConfig().BufferSize)
	b := paxi.NewBallot(1, "1.1")
	p.ballot = b
	p.IsFastMode = true
	p.Q2f = func(q *paxi.Quorum) bool { return q.Size() >= 7 }
	p.Q2fSize = 7

	numSlots := 3
	p.resolutionLog = make(map[int]*CommandSet, numSlots)
	for slot := 1; slot <= numSlots; slot++ {
		p.resolutionLog[slot] = NewCommandSet(10, true, false)
	}
	v := []paxi.Value{paxi.GenerateRandVal(8), paxi.GenerateRandVal(8), paxi.GenerateRandVal(8)}

	slotOrder := []int{      1,     1,     1,     1,     1,     2,     2,     3,     1,     1,     2,     1,     2,     1,     2,     2,     3,     2,     3}
	nodeOrder := []paxi.ID{"1.1", "1.3", "1.9", "1.6", "1.7", "1.3", "1.6", "1.6", "1.4", "1.8", "1.7", "1.2", "1.1", "1.5", "1.2", "1.5", "1.2", "1.9", "1.9"}
	cmdOrder := []int{       1,     1,     2,     1,     3,     2,     3,     2,     1,     1,     2,     1,     2,     1,     2,     3,     3,     1,     3}
	//expQPossible := []bool{true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true,  true, true,   false,  true, false, false}

	// (1) vote Cmd 1 on slot 1 from node 1.1
	for i, slot := range slotOrder {
		cmd := paxi.Command{Key: paxi.Key(cmdOrder[i]), Value: v[cmdOrder[i] - 1], ClientID: "1.1", CommandID: cmdOrder[i]}
		p2b := P2b{
			Ballot:        b,
			Command:       cmd,
			ID:            nodeOrder[i],
			Slot:          slot,
			ClassicQuourm: false,
		}

		p.HandleP2b(p2b)
	}

	assert.Equal(t, 2, len(p.conflictSlots))
	assert.Equal(t, true, p.conflictSlots[2]) // indicates that it is resolving
	assert.Equal(t, true, p.conflictSlots[3])
}
