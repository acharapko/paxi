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

Currently, we do not implement a full Replicated State Machine (RSM), 
and instead implement a version of Fast (Flexible) Paxos that decides
on many different instance of consensus under the same leader without
having a perfect, gap-free log for the state machine. 

Further improvement will add a full RSM implementation.

