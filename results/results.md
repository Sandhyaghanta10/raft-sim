5-node cluster, 50 seeds per row, 10000 logical ticks per run (election timeout 150-300, heartbeat 50, message delay 1-10, one client command every 20 ticks).

### 1. Message loss

No crashes, no partitions. 2% of messages are also duplicated.

| Message loss | Safety violations | Commands committed / 1000 ticks | Elections / run | Time with no usable leader | Mean outage (ticks) | Longest outage (ticks) |
|---|---:|---:|---:|---:|---:|---:|
| 0% | 0 / 50 | 49.0 | 1.3 | 0.00% | - | 0 |
| 5% | 0 / 50 | 49.0 | 1.4 | 0.00% | - | 0 |
| 10% | 0 / 50 | 48.9 | 1.4 | 0.00% | - | 0 |
| 20% | 0 / 50 | 48.8 | 1.7 | 0.00% | - | 0 |
| 30% | 0 / 50 | 48.3 | 2.9 | 0.64% | 637 | 1138 |
| 40% | 0 / 50 | 47.4 | 5.2 | 1.20% | 427 | 879 |

### 2. Node crashes

5% message loss, 2% duplicates. Crash rate is the chance per tick that one random live node crashes; it restarts after 50-200 ticks with only its persistent state (term, vote, log).

| Crash rate / tick | Safety violations | Commands committed / 1000 ticks | Elections / run | Time with no usable leader | Mean outage (ticks) | Longest outage (ticks) |
|---|---:|---:|---:|---:|---:|---:|
| 0.0000 | 0 / 50 | 49.0 | 1.4 | 0.00% | - | 0 |
| 0.0005 | 0 / 50 | 48.0 | 2.6 | 1.91% | 203 | 435 |
| 0.0010 | 0 / 50 | 47.2 | 3.5 | 3.44% | 189 | 447 |
| 0.0020 | 0 / 50 | 44.7 | 6.7 | 8.55% | 209 | 751 |
| 0.0050 | 0 / 50 | 38.5 | 13.1 | 21.40% | 209 | 1029 |

### 3. Does the checker catch real bugs?

Hard scenario (10% loss, 5% duplicates, crash rate 0.02, a partition every 400 ticks lasting 150), 1000 seeds per variant. Each variant disables one safety rule of Raft.

| Variant | Runs with a violation | First violation found |
|---|---:|---|
| Correct Raft | 0 / 1000 | none |
| Correct Raft, no no-op entry (control) | 0 / 1000 | none |
| Skip the log up-to-date check when voting | 867 / 1000 | seed 0: LeaderCompleteness @tick 1578: leader 4 of term 3 lacks entry 9 LogEntry[term=1, command=8] committed in term 1 |
| Forget the vote after a restart | 18 / 1000 | seed 3: ElectionSafety @tick 6737: term 15 elected both node 4 and node 0 |
| Commit old-term entries by counting replicas | 0 / 1000 | none |
| Commit old-term entries, and no no-op entry | 0 / 1000 | none |
