package raftsim;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Watches a run from the outside and records the first violated safety property from the Raft paper (Figure 3).
 * It never influences the nodes, it only reads them.
 *
 * <ul>
 *   <li><b>Election Safety</b>: at most one leader is ever elected in a given term.</li>
 *   <li><b>Leader Completeness</b>: a leader contains every entry committed in an earlier term.</li>
 *   <li><b>State Machine Safety</b>: nobody ever commits a different command at the same index, and a committed
 *       entry never disappears from a node's log.</li>
 *   <li><b>Log Matching</b>: if two logs hold an entry with the same index and term, the logs are identical up to it.</li>
 * </ul>
 */
public final class InvariantChecker {
    /** The commit term is when a leader committed the entry; used to know which later leaders must contain it. */
    private record Committed(LogEntry entry, int commitTerm) {}

    private static final int PERIODIC_CHECK_EVERY = 10;

    private final Map<Integer, Integer> leaderOfTerm = new HashMap<>();
    private final Map<Integer, Committed> committed = new HashMap<>();
    private final Map<RaftNode, Integer> verifiedUpTo = new IdentityHashMap<>();
    private Violation first;

    public Violation first() {
        return first;
    }

    public int committedCommands() {
        return (int) committed.values().stream().filter(c -> c.entry().command() != LogEntry.NOOP).count();
    }

    /** Called the moment a node wins an election (so a leader that is deposed within the same tick is still seen). */
    public void onLeaderElected(RaftNode leader, long tick) {
        Integer previous = leaderOfTerm.putIfAbsent(leader.currentTerm(), leader.id());
        if (previous != null && previous != leader.id()) {
            fail(Violation.ELECTION_SAFETY, tick,
                    "term " + leader.currentTerm() + " elected both node " + previous + " and node " + leader.id());
        }
        for (Map.Entry<Integer, Committed> e : committed.entrySet()) {
            Committed c = e.getValue();
            if (leader.currentTerm() > c.commitTerm() && !holds(leader, e.getKey(), c.entry())) {
                fail(Violation.LEADER_COMPLETENESS, tick, "leader " + leader.id() + " of term " + leader.currentTerm()
                        + " lacks entry " + e.getKey() + " " + c.entry() + " committed in term " + c.commitTerm());
            }
        }
    }

    /** Called at the end of every tick with the live nodes (crashed nodes are null). */
    public void check(long tick, RaftNode[] nodes) {
        if (first != null) {
            return;
        }
        recordNewCommits(tick, nodes, true);   // leaders first, so commitTerm is the committing leader's term
        recordNewCommits(tick, nodes, false);
        if (first == null && tick % PERIODIC_CHECK_EVERY == 0) {
            recheckCommittedPrefixes(tick, nodes);
            checkLogMatching(tick, nodes);
        }
    }

    private void recordNewCommits(long tick, RaftNode[] nodes, boolean leadersOnly) {
        for (RaftNode node : nodes) {
            if (node == null || (node.role() == RaftNode.Role.LEADER) != leadersOnly) {
                continue;
            }
            int verified = verifiedUpTo.getOrDefault(node, 0);
            for (int i = verified + 1; i <= node.commitIndex(); i++) {
                if (i > node.lastLogIndex()) {
                    fail(Violation.STATE_MACHINE_SAFETY, tick, "node " + node.id() + " committed index " + i + " but its log ends at " + node.lastLogIndex());
                    return;
                }
                LogEntry entry = node.entryAt(i);
                Committed existing = committed.get(i);
                if (existing == null) {
                    committed.put(i, new Committed(entry, node.currentTerm()));
                } else if (!existing.entry().equals(entry)) {
                    fail(Violation.STATE_MACHINE_SAFETY, tick, "index " + i + " committed as " + existing.entry()
                            + " elsewhere but node " + node.id() + " committed " + entry);
                    return;
                }
            }
            verifiedUpTo.put(node, Math.max(verified, node.commitIndex()));
        }
    }

    /** Committed entries must stay put: re-verify each live node's whole committed prefix. */
    private void recheckCommittedPrefixes(long tick, RaftNode[] nodes) {
        for (RaftNode node : nodes) {
            if (node == null) {
                continue;
            }
            for (int i = 1; i <= node.commitIndex(); i++) {
                Committed c = committed.get(i);
                if (c != null && !holds(node, i, c.entry())) {
                    fail(Violation.STATE_MACHINE_SAFETY, tick, "committed entry " + i + " " + c.entry() + " is missing or changed on node " + node.id());
                    return;
                }
            }
        }
    }

    private void checkLogMatching(long tick, RaftNode[] nodes) {
        for (int a = 0; a < nodes.length; a++) {
            for (int b = a + 1; b < nodes.length; b++) {
                if (nodes[a] == null || nodes[b] == null) {
                    continue;
                }
                int sameTermAt = highestSameTermIndex(nodes[a], nodes[b]);
                for (int i = 1; i <= sameTermAt; i++) {
                    if (!nodes[a].entryAt(i).equals(nodes[b].entryAt(i))) {
                        fail(Violation.LOG_MATCHING, tick, "nodes " + a + " and " + b + " agree on term at index " + sameTermAt + " but differ at index " + i);
                        return;
                    }
                }
            }
        }
    }

    private static int highestSameTermIndex(RaftNode x, RaftNode y) {
        for (int i = Math.min(x.lastLogIndex(), y.lastLogIndex()); i >= 1; i--) {
            if (x.entryAt(i).term() == y.entryAt(i).term()) {
                return i;
            }
        }
        return 0;
    }

    private static boolean holds(RaftNode node, int index, LogEntry entry) {
        return node.lastLogIndex() >= index && node.entryAt(index).equals(entry);
    }

    private void fail(String kind, long tick, String detail) {
        if (first == null) {
            first = new Violation(kind, tick, detail);
        }
    }
}
