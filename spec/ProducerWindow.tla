---------------------------- MODULE ProducerWindow ----------------------------
(***************************************************************************)
(* The idempotent producer's in-flight window, and the one rule that makes  *)
(* it safe.                                                                 *)
(*                                                                          *)
(* Kafka lets a client keep several `Produce` requests in flight on one      *)
(* connection. The broker processes a connection's requests in order, so a   *)
(* partition's batches arrive in sequence even though several are           *)
(* outstanding. The cost is recovery: if the r-th request fails, the broker  *)
(* never wrote it, so every later request in that window was rejected for    *)
(* being out of sequence -- whatever it happened to answer.                  *)
(*                                                                          *)
(* `kestrel` therefore retires only the *contiguous leading run* of          *)
(* successes and re-sends the rest, in order. `kestrel-client`'s adversarial *)
(* tests check that on scenarios I chose by hand. Hand-chosen scenarios are  *)
(* hand-chosen; this checks the invariant over every interleaving TLC can    *)
(* reach within the bounds below.                                            *)
(*                                                                          *)
(* Deliberately *not* modelled: leadership changes, coordinators,            *)
(* transactions, the network. This is one partition's sequencing and the     *)
(* client's retire rule, which is the part where being wrong is silent --    *)
(* a gap or a duplicate in the log that still returns Ok.                    *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    Batches,      \* how many batches the caller enqueues
    MaxInFlight,  \* the window: Kafka allows 5 with idempotence, as do we
    MaxFailures   \* bound on injected failures, to bound the state space

VARIABLES
    pending,   \* batches not yet acknowledged, in the order they were enqueued
    log,       \* what the broker has durably written, in order
    failures   \* failures injected so far

vars == <<pending, log, failures>>

Min(a, b) == IF a < b THEN a ELSE b

Init ==
    /\ pending = [i \in 1..Batches |-> i]
    /\ log = << >>
    /\ failures = 0

(***************************************************************************)
(* One round trip of the whole window.                                      *)
(*                                                                          *)
(* `k` is how many of the window's requests the broker accepted before the   *)
(* first failure. k = w means the window succeeded outright. k < w means     *)
(* request k+1 failed and requests k+2..w were rejected behind it -- which   *)
(* is why they are *not* retired, and why this is the only interesting step  *)
(* in the model.                                                            *)
(***************************************************************************)
SendWindow ==
    /\ Len(pending) > 0
    /\ LET w == Min(MaxInFlight, Len(pending)) IN
       \E k \in 0..w :
          /\ k = w \/ failures < MaxFailures
          /\ log' = log \o SubSeq(pending, 1, k)
          /\ pending' = SubSeq(pending, k + 1, Len(pending))
          /\ failures' = IF k < w THEN failures + 1 ELSE failures

Done ==
    /\ Len(pending) = 0
    /\ UNCHANGED vars

Next == SendWindow \/ Done

Spec == Init /\ [][Next]_vars /\ WF_vars(SendWindow)

(***************************************************************************)
(* Safety: the log is exactly 1, 2, 3, ... at all times.                     *)
(*                                                                          *)
(* One predicate covers all three ways a pipelined producer goes wrong: a    *)
(* gap (a batch retired that the broker never wrote), a duplicate (a batch   *)
(* re-sent after it was written), and reordering.                            *)
(***************************************************************************)
LogIsExactPrefix == log = [i \in 1..Len(log) |-> i]

(***************************************************************************)
(* Liveness: with failures bounded, every batch is eventually written.       *)
(* This is what rules out the "safe by never making progress" cheat.         *)
(***************************************************************************)
AllEventuallyWritten == <>(Len(log) = Batches)

(***************************************************************************)
(* The mutation, for anyone who doubts the invariant has teeth: replace the  *)
(* `log' = log \o SubSeq(pending, 1, k)` above with a rule that also retires *)
(* the requests *behind* the failure --                                      *)
(*                                                                          *)
(*     log' = log \o SubSeq(pending, 1, k) \o SubSeq(pending, k+2, w)        *)
(*     pending' = SubSeq(pending, k+2, Len(pending))                         *)
(*                                                                          *)
(* -- which is the same weakening that makes `kestrel`'s two ordering tests  *)
(* fail, and TLC reports a gap in `LogIsExactPrefix` within a few states.    *)
(***************************************************************************)
=============================================================================
