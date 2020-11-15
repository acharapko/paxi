package fastpaxos

import (
	"fmt"
	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type UniqueCommandId struct {
	ClientId      paxi.ID
	CommandId     int
}

func UniqueCommandIdFromCommand(cmd *paxi.Command) UniqueCommandId {
	return UniqueCommandId{
		ClientId:  cmd.ClientID,
		CommandId: cmd.CommandID,
	}
}

func (ucid UniqueCommandId) String() string {
	return fmt.Sprintf("UCID{client=%v cmdid=%d}", ucid.ClientId, ucid.CommandId)
}

type CommandSet struct {
	commandSet map[UniqueCommandId] *paxi.Command

	// special case to support vote counting for each command in the set
	quorums      map[UniqueCommandId]*paxi.Quorum
	totalVotes   *paxi.Quorum          // total votes among all commands in the set
	maxQ         *paxi.Quorum // quorum with max votes
	maxUCId      UniqueCommandId
	countQuorums bool

	// special case to support vote counting for each command per slot
	slotQuorums map[UniqueCommandId]map[int]*paxi.Quorum
}

func NewCommandSet(initSize int, countQuorums, quorumPerSlot bool) *CommandSet {
	cs := &CommandSet{
		commandSet: make(map[UniqueCommandId]*paxi.Command, initSize),
		//filter:      make(map[int]uint8, initSize),
		countQuorums: countQuorums,
		totalVotes:   paxi.NewQuorum(),
	}

	if cs.countQuorums {
		cs.quorums = make(map[UniqueCommandId]*paxi.Quorum, initSize)
	}

	if quorumPerSlot {
		cs.slotQuorums = make(map[UniqueCommandId]map[int]*paxi.Quorum, initSize)
	}

	return cs
}

func (cs *CommandSet) Exists(cmd *paxi.Command) bool {
	cmdUniqueId := UniqueCommandId{
		ClientId:  cmd.ClientID,
		CommandId: cmd.CommandID,
	}
	return cs.commandSet[cmdUniqueId] != nil
}

func (cs *CommandSet) ExistsByUCID(ucid UniqueCommandId) bool {
	return cs.commandSet[ucid] != nil
}

func (cs *CommandSet) Get(ucid UniqueCommandId) *paxi.Command {
	return cs.commandSet[ucid]
}

func (cs *CommandSet) GetQuorum(ucid UniqueCommandId) *paxi.Quorum {
	return cs.quorums[ucid]
}

/**
 * This adds the command to a set and applies vote to the command. If command is already in the set, the vote is still
 * applied.
 * return Command's Quorum, ucid of max voted command, size of maxQuorum, total votes
 */
func (cs *CommandSet) AddWithVote(cmd *paxi.Command, voteId paxi.ID) (*paxi.Quorum, UniqueCommandId, int, int) {
	if !cs.countQuorums {
		log.Fatalf("This commandSet is not in quorum counting mode. You must use Add method instead")
		return nil, cs.maxUCId, 0, 0
	}

	cmdUniqueId := UniqueCommandId{
		ClientId:  cmd.ClientID,
		CommandId: cmd.CommandID,
	}

	if q, exists := cs.quorums[cmdUniqueId]; exists {
		cs.vote(q, voteId, cmdUniqueId)
	} else {
		cs.commandSet[cmdUniqueId] = cmd
		cs.quorums[cmdUniqueId] = paxi.NewQuorum()
		cs.vote(cs.quorums[cmdUniqueId], voteId, cmdUniqueId)
	}

	return cs.quorums[cmdUniqueId], cs.maxUCId, cs.maxQ.Size(), cs.totalVotes.Size()
}

func (cs *CommandSet) AddVotesFromAnotherCommandSet(cs2 *CommandSet, maxThreshold int) {
	for i, q2 := range cs2.quorums {
		q1 := cs.quorums[i]
		if q1 != nil && q2.Size() < maxThreshold {
			q1.AddFromQuorum(q2)
		}
	}
}

func (cs *CommandSet) Add(cmd *paxi.Command) {

	cmdUniqueId := UniqueCommandId{
		ClientId:  cmd.ClientID,
		CommandId: cmd.CommandID,
	}

	cs.commandSet[cmdUniqueId] = cmd
	if cs.countQuorums && cs.quorums[cmdUniqueId] == nil {
		cs.quorums[cmdUniqueId] = paxi.NewQuorum()
	}
}

func (cs *CommandSet) Remove(cmd *paxi.Command) {
	cmdUniqueId := UniqueCommandId{
		ClientId:  cmd.ClientID,
		CommandId: cmd.CommandID,
	}

	delete(cs.commandSet, cmdUniqueId)
	if cs.countQuorums {
		delete(cs.quorums, cmdUniqueId)
	}

	if cs.slotQuorums != nil {
		delete(cs.slotQuorums, cmdUniqueId)
	}
}

func (cs *CommandSet) Intersect(cs2 *CommandSet) *CommandSet {
	intersect := NewCommandSet(len(cs.commandSet), false, false)
	for uniqueId, cmd1 := range cs.commandSet {
		if _, exists := cs2.commandSet[uniqueId]; exists {
			// both sets have cmd1
			intersect.Add(cmd1)
		}
	}

	return intersect
}

func (cs *CommandSet) Union(cs2 *CommandSet) *CommandSet {
	union := cs.Clone(false)
	for _, cmd := range cs2.commandSet {
		union.Add(cmd)
	}

	return union
}

/**
 * relative compliment
 * s \cs2 = {x in cs | x not in cs2}
 **/
func (cs *CommandSet) Difference(cs2 *CommandSet) *CommandSet {
	diff := cs.Clone(false)
	for _, cmd := range cs2.commandSet {
		diff.Remove(cmd)
	}

	return diff
}


func (cs *CommandSet) Clone(cloneVotes bool) *CommandSet {
	clone := &CommandSet{
		commandSet:   make(map[UniqueCommandId]*paxi.Command, len(cs.commandSet)),
		countQuorums: cs.countQuorums && cloneVotes,
		totalVotes:   cs.totalVotes,
		maxQ:         cs.maxQ,
	}

	if cs.countQuorums && cloneVotes {
		clone.quorums = make(map[UniqueCommandId]*paxi.Quorum)
	}

	for k, v := range cs.commandSet {
		clone.commandSet[k] = v
		if cs.countQuorums && cloneVotes {
			clone.quorums[k] = cs.quorums[k]
		}
	}
	return clone
}

func (cs *CommandSet) AddSlotQuorum(cmd *paxi.Command, slot int, q *paxi.Quorum) {
	cmdid := UniqueCommandIdFromCommand(cmd)
	if sq, exists := cs.slotQuorums[cmdid]; exists {
		sq[slot] = q
	} else {
		cs.slotQuorums[cmdid] = make(map[int]*paxi.Quorum)
		cs.slotQuorums[cmdid][slot] = q
	}
}

func (cs *CommandSet) AddSlotQuorumIfDoesNotExist(cmd *paxi.Command, slot int, q *paxi.Quorum) {
	cmdid := UniqueCommandIdFromCommand(cmd)
	if sq, exists := cs.slotQuorums[cmdid]; exists {
		if sq[slot] == nil {
			sq[slot] = q
		}
	} else {
		cs.slotQuorums[cmdid] = make(map[int]*paxi.Quorum)
		cs.slotQuorums[cmdid][slot] = q
	}
}

func (cs *CommandSet) vote(q *paxi.Quorum, nodeId paxi.ID, ucid UniqueCommandId) {
	q.ACK(nodeId)
	cs.totalVotes.ACK(nodeId)
	if cs.maxQ == nil {
		cs.maxQ = q
		cs.maxUCId = ucid
	} else if cs.maxQ.Size() < q.Size() {
		cs.maxQ = q
		cs.maxUCId = ucid
	}
}