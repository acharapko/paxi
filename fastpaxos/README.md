## What is Fast Paxos and Fast Flexible Paxos?

**Fast Paxos** is the Paxos flavor by Leslie Lamport that optimizes for
 lower latency in happy case by allowing clients to directly write to
  the acceptors. In the happy case, most acceptors receive the same command
  from the clients for the same slot/instance. In some cases, however, 
  conflicts may arise where more than one command can exist in the same 
  instance _i_. Normally, a quorum larger than the majority quorum (fast
  quorum), is needed to complete an instance of Paxos without conflicts. 
  In case of a conflict, when none of the commands has reached the fast
  quorum, a leader can recover the instance by running a classical Paxos.
  
  **Fast Flexible Paxos** optimizes Fast Paxos with Flexible Quorums to 
  allow adjusting quorum sizes and achieving better performance through a
  smaller fast quorum. Smaller quorum not only reduces the latency, but
  also reduce the conflict rate, further improving performance by reducing
  the need in recovery with classical Paxos    

## What is included?

Under Fast Paxos we Include:
- [x] Fast Paxos
- [x] Fast Flexible Paxos

## What is implemented?

This implementation is a stripped-down prototype of Fast Flexible Paxos as used in a Replicated State Machine(RSM). 
RSM uses a log to ensure the same operations are applied to each replica in the same order. Fast Paxos RSM tries to
commit each slot in a fast quorum, and whenever fast quorum on a slot is not possible, it resolves the slot 
using the leader conflict resolution of Fast Paxos. RSM implementation ensures that each command appears in at most 
one slot.

## What is missing?

**So far we implement a happy case and the RSM stalls when a fast quorum of nodes in not available**

The fix should be an easy time-out to check if enough nodes have replied on some slot, and if not, force the resolution

## How to Run?

./server -log_dir=logs -log_level=info -id $1 -algorithm=fastpaxos -p1q=$2 -p2qf=$3 -p2qc=$4 

- p1q is the size of phase-1 quorum
- p2qf is the size of phase-2 fast quorum
- p2qc is the size of phase-2 classic quorum

To run as regular Fast Paxos, adjust the quorum size accordingly


