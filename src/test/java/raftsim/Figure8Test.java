package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * The Figure 8 scenario from the Raft paper, replayed by hand (no randomness, no timing luck).
 *
 * <p>Five servers S1..S5 (ids 0..4). Starting state:
 * <pre>
 *   S1, S2: log [1, 2]   (entry 2 was written by a term-2 leader and reached only S1 and S2)
 *   S3, S4: log [1]
 *   S5    : log [1, 3]   (a term-3 leader wrote entry 2 and crashed before replicating it)
 * </pre>
 * Phase 1: S1 wins term 4 with votes from S2 and S3 and replicates entry 2 to S3, so entry 2 sits on S1, S2, S3.
 * A buggy leader that counts replicas for that old-term entry commits it.
 * Phase 2: S1 crashes. S5 (last log term 3, newer than everyone's 2) wins term 5 and overwrites entry 2 on a majority.
 * A committed entry has been lost.
 *
 * <p>Random fault injection never produced this in over 20,000 runs; a directed schedule produces it every time.
 */
class Figure8Test {
    private record Packet(int from, int to, Message message) {}

    private final List<Packet> wire = new ArrayList<>();
    private final RaftNode[] nodes = new RaftNode[5];
    private final InvariantChecker checker = new InvariantChecker();
    private long tick;

    private void build(RaftConfig cfg) {
        int[][] logs = {{1, 2}, {1, 2}, {1}, {1}, {1, 3}};
        int[] terms = {3, 3, 3, 3, 4};
        RaftNode.Hooks hooks = new RaftNode.Hooks() {
            @Override
            public void becameLeader(RaftNode node) {
                checker.onLeaderElected(node, tick);
            }
        };
        for (int id = 0; id < 5; id++) {
            PersistentState ps = new PersistentState();
            ps.currentTerm = terms[id];
            for (int term : logs[id]) {
                ps.log.add(new LogEntry(term, 100 + ps.log.size() + 1));
            }
            int self = id;
            nodes[id] = new RaftNode(id, 5, ps, cfg, new Random(id), (to, m) -> wire.add(new Packet(self, to, m)), hooks);
        }
    }

    /** Deliver messages until none remain between live nodes; packets touching a crashed node are lost. */
    private void pump(Set<Integer> live) {
        while (!wire.isEmpty()) {
            Packet p = wire.remove(0);
            if (live.contains(p.from()) && live.contains(p.to())) {
                tick++;
                nodes[p.to()].handle(p.from(), p.message());
                checker.check(tick, nodes);
            }
        }
    }

    private void tickUntilCandidate(RaftNode node) {
        while (node.role() != RaftNode.Role.CANDIDATE) {
            node.tick();
        }
    }

    private void runFigure8(RaftConfig cfg) {
        build(cfg);
        tickUntilCandidate(nodes[0]);                 // phase 1: S1 campaigns in term 4 among S1, S2, S3
        pump(Set.of(0, 1, 2));
        assertEquals(RaftNode.Role.LEADER, nodes[0].role());
        assertEquals(4, nodes[0].currentTerm());

        tickUntilCandidate(nodes[4]);                 // phase 2: S1 is gone; S5 campaigns in term 5 among S2..S5
        pump(Set.of(1, 2, 3, 4));
    }

    @Test
    void buggyCommitRuleLosesACommittedEntryAndTheCheckerCatchesIt() {
        runFigure8(RaftConfig.correct().withCommitOfOldTerms().withoutNoOp());
        assertEquals(2, nodes[0].commitIndex(), "buggy S1 committed the term-2 entry by counting replicas");
        assertEquals(RaftNode.Role.LEADER, nodes[4].role(), "S5 is allowed to win: its log looks newest");
        assertNotNull(checker.first());
        assertEquals(Violation.LEADER_COMPLETENESS, checker.first().kind());
    }

    @Test
    void correctCommitRuleSurvivesTheSameSchedule() {
        runFigure8(RaftConfig.correct().withoutNoOp());
        assertEquals(0, nodes[0].commitIndex(), "an old-term entry on a majority is still not committed");
        assertEquals(RaftNode.Role.LEADER, nodes[4].role());
        assertNull(checker.first(), "nothing was committed, so S5 overwriting entry 2 loses nothing");
    }

    @Test
    void theNoOpEntryAlsoDefusesTheSchedule() {
        // Even with the buggy rule, a leader that appends a no-op replicates a term-4 entry along with the old one,
        // so S5 (last term 3) can no longer win. This is why random testing of my default config never found the bug.
        runFigure8(RaftConfig.correct().withCommitOfOldTerms());
        assertNull(checker.first());
        assertEquals(RaftNode.Role.CANDIDATE, nodes[4].role(), "S5 cannot collect a majority of votes");
    }
}
