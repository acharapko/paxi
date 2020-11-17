package fastpaxos

import (
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestCommandSet_Add(t *testing.T) {
	cs := NewCommandSet(10, false, false)

	for i := 0; i < 10; i++ {
		v := paxi.GenerateRandVal(8)
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}

		cs.Add(cmd)
		assert.True(t, cs.Exists(cmd))
	}

	assert.Equal(t, 10, len(cs.commandSet))

	v1 := paxi.GenerateRandVal(8)
	cmd1:= &paxi.Command{
		Key:       40,
		Value:     v1,
		ClientID:  "1.1",
		CommandID: 41,
	}

	cmd2:= &paxi.Command{
		Key:       40,
		Value:     v1,
		ClientID:  "1.1",
		CommandID: 41,
	}

	cs.Add(cmd1)
	assert.Equal(t, 11, len(cs.commandSet))

	cs.Add(cmd2)
	assert.Equal(t, 11, len(cs.commandSet))

	assert.True(t, cs.Exists(cmd1))
}

func TestCommandSet_Remove(t *testing.T) {
	cs := NewCommandSet(10, false, false)
	cmds := make([]*paxi.Command, 10)
	for i := 0; i < 10; i++ {
		v := paxi.GenerateRandVal(8)
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}
		cmds[i] = cmd
		cs.Add(cmd)
		assert.True(t, cs.Exists(cmd))
	}

	assert.Equal(t, 10, len(cs.commandSet))

	// try to remove non-existent item
	v1 := paxi.GenerateRandVal(8)
	cmd1:= &paxi.Command{
		Key:       40,
		Value:     v1,
		ClientID:  "1.1",
		CommandID: 41,
	}

	cs.Remove(cmd1)
	assert.Equal(t, 10, len(cs.commandSet))

	expectedCount := 10
	for _, cmd := range cmds {
		expectedCount -= 1
		cs.Remove(cmd)
		assert.Equal(t, expectedCount, len(cs.commandSet))
	}
}

func TestCommandSet_Intersect(t *testing.T) {
	cs1 := NewCommandSet(20, false, false)
	cs2 := NewCommandSet(20, false, false)
	intersectCheck := make([]*paxi.Command, 0)
	for i := 0; i < 20; i++ {
		v := paxi.GenerateRandVal(8)
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}

		r1 := rand.Intn(2)
		r2 := rand.Intn(2)
		if r1 > 0 {
			cs1.Add(cmd)
		}
		if r2 > 0 {
			cs2.Add(cmd)
		}

		if r1 > 0 && r2 > 0 {
			intersectCheck = append(intersectCheck, cmd)
		}
	}

	intersect := cs1.Intersect(cs2)
	log.Debugf("Intersection: %v. Expected: %v", intersect.commandSet, intersectCheck)
	assert.Equal(t, len(intersectCheck), len(intersect.commandSet))
	for _, cmd := range intersectCheck {
		assert.True(t, intersect.Exists(cmd))
	}
}

func TestCommandSet_Union(t *testing.T) {
	cs1 := NewCommandSet(20, false, false)
	cs2 := NewCommandSet(20, false, false)
	unionCheck := make([]*paxi.Command, 0)
	for i := 0; i < 20; i++ {
		v := paxi.GenerateRandVal(8)
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}

		r1 := rand.Intn(2)
		r2 := rand.Intn(2)
		if r1 > 0 {
			cs1.Add(cmd)
		}
		if r2 > 0 {
			cs2.Add(cmd)
		}

		if r1 > 0 || r2 > 0 {
			unionCheck = append(unionCheck, cmd)
		}
	}

	union := cs1.Union(cs2)
	assert.Equal(t, len(unionCheck), len(union.commandSet))
	for _, cmd := range unionCheck {
		assert.True(t, union.Exists(cmd))
	}
}

func TestCommandSet_AddVotesFromAnotherCommandSet(t *testing.T) {
	cs1 := NewCommandSet(20, true, false)
	cs2 := NewCommandSet(20, true, false)
	v := paxi.GenerateRandVal(8)

	ucids := make([]UniqueCommandId, 0)
	for i := 0; i < 5; i++ {
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}
		ucids = append(ucids, UniqueCommandIdFromCommand(cmd))
		cs1.Add(cmd)
		for j := 1; j < 4; j++ {
			cs2.AddWithVote(cmd, paxi.NewID(1, j))
		}
	}

	for _, ucid := range ucids {
		assert.Equal(t, 0, cs1.quorums[ucid].Size())
		assert.Equal(t, 3, cs2.quorums[ucid].Size())
	}

	cs1.AddVotesFromAnotherCommandSet(cs2, 6)

	for _, ucid := range ucids {
		assert.Equal(t, 3, cs1.quorums[ucid].Size())
		assert.Equal(t, 3, cs2.quorums[ucid].Size())
	}

	for i := 0; i < 5; i++ {
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}
		cs1.Add(cmd)
		for j := 8; j < 10; j++ {
			cs2.AddWithVote(cmd, paxi.NewID(1, j))
		}
	}

	for _, ucid := range ucids {
		assert.Equal(t, 3, cs1.quorums[ucid].Size())
		assert.Equal(t, 5, cs2.quorums[ucid].Size())
	}

	cs1.AddVotesFromAnotherCommandSet(cs2, 6)

	for _, ucid := range ucids {
		assert.Equal(t, 5, cs1.quorums[ucid].Size())
		assert.Equal(t, 5, cs2.quorums[ucid].Size())
	}

}


func BenchmarkCommandSet_Remove(b *testing.B) {
	v := paxi.GenerateRandVal(8)
	cs := NewCommandSet(b.N, false, false)
	cmds := make([]*paxi.Command, b.N)
	for i := 0; i < b.N; i++ {
		cmd := &paxi.Command{
			Key:       paxi.Key(i % 5),
			Value:     v,
			ClientID:  "1.1",
			CommandID: i,
		}
		cmds[i] = cmd
		cs.Add(cmd)
	}

	log.Debugf("Starting Removals. b.N = %d", b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cs.Remove(cmds[i])
	}
}
