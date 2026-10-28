package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

/** End-to-end behaviour of whole clusters under the simulator. */
class ClusterTest {
    static Scenario hard(long ticks) {
        return Scenario.baseline().withTicks(ticks).withDrop(0.10).withDuplicates(0.05)
                .withCrashes(0.01).withPartitions(400, 150);
    }

    private static void runTicks(Cluster cluster, int n) {
        for (int i = 0; i < n; i++) {
            cluster.tick();
        }
    }

    private static Cluster clusterWithLeader(long seed) {
        Cluster cluster = new Cluster(Scenario.baseline(), RaftConfig.correct(), seed);
        for (int i = 0; i < 3000 && cluster.leader() == null; i++) {
            cluster.tick();
        }
        assertNotNull(cluster.leader(), "a leader should be elected quickly in a healthy cluster");
        return cluster;
    }

    @Test
    void healthyClusterElectsOneLeaderAndCommitsClientCommands() {
        Cluster cluster = new Cluster(Scenario.baseline().withTicks(3000), RaftConfig.correct(), 1);
        RunResult result = cluster.run();
        assertTrue(result.safe(), String.valueOf(result.violation()));
        assertTrue(result.committedCommands() >= 120, "committed " + result.committedCommands());
        assertEquals(0, result.unavailableTicks());
        long leaders = java.util.stream.IntStream.range(0, 5)
                .filter(i -> cluster.node(i).role() == RaftNode.Role.LEADER).count();
        assertEquals(1, leaders);
    }

    @Test
    void sameSeedReplaysIdenticallyAndDifferentSeedsDiffer() {
        RunResult a = new Cluster(hard(4000), RaftConfig.correct(), 11).run();
        RunResult b = new Cluster(hard(4000), RaftConfig.correct(), 11).run();
        assertEquals(a, b, "deterministic simulation: same seed must give the same run");
        assertNotEquals(a, new Cluster(hard(4000), RaftConfig.correct(), 12).run());
    }

    @Test
    void crashedLeaderIsReplacedByANewLeaderInALaterTerm() {
        Cluster cluster = clusterWithLeader(3);
        int oldId = cluster.leader().id();
        int oldTerm = cluster.leader().currentTerm();
        cluster.crash(oldId);
        runTicks(cluster, 1500);
        RaftNode replacement = cluster.leader();
        assertNotNull(replacement, "the remaining 4 nodes must elect a new leader");
        assertNotEquals(oldId, replacement.id());
        assertTrue(replacement.currentTerm() > oldTerm);
        assertNull(cluster.violation());
    }

    @Test
    void restartedNodeCatchesUpWithTheCommittedLog() {
        Cluster cluster = clusterWithLeader(5);
        int follower = (cluster.leader().id() + 1) % 5;
        cluster.crash(follower);
        runTicks(cluster, 1000);                       // the cluster keeps committing without it
        int committedWhileDown = cluster.leader().commitIndex();
        cluster.restart(follower);
        runTicks(cluster, 600);
        RaftNode rejoined = cluster.node(follower);
        RaftNode leader = cluster.leader();
        assertTrue(rejoined.commitIndex() >= committedWhileDown, "caught up to what was committed while it was down");
        for (int i = 1; i <= rejoined.commitIndex(); i++) {
            assertEquals(leader.entryAt(i), rejoined.entryAt(i));
        }
        assertNull(cluster.violation());
    }

    @Test
    void isolatedLeaderCannotCommitAndStepsDownAfterTheHeal() {
        Cluster cluster = clusterWithLeader(7);
        RaftNode stale = cluster.leader();
        int staleId = stale.id();
        int committedBefore = stale.commitIndex();
        cluster.isolate(Set.of(staleId));
        runTicks(cluster, 1200);

        RaftNode fresh = cluster.leader();             // highest-term leader = the one on the majority side
        assertNotEquals(staleId, fresh.id());
        assertTrue(fresh.currentTerm() > stale.currentTerm());
        assertEquals(committedBefore, stale.commitIndex(), "a minority leader must not be able to commit");

        cluster.heal();
        runTicks(cluster, 600);
        assertEquals(RaftNode.Role.FOLLOWER, stale.role(), "the old leader must step down on seeing a higher term");
        assertNull(cluster.violation());
        for (int i = 1; i <= Math.min(stale.commitIndex(), fresh.commitIndex()); i++) {
            assertEquals(fresh.entryAt(i), stale.entryAt(i));
        }
    }

    @Test
    void correctRaftNeverViolatesSafetyUnderHeavyFaults() {
        for (int seed = 0; seed < 40; seed++) {
            RunResult r = new Cluster(hard(6000), RaftConfig.correct(), seed).run();
            assertTrue(r.safe(), "seed " + seed + ": " + r.violation());
        }
    }

    @Test
    void correctRaftWithoutNoOpIsStillSafe() {
        for (int seed = 0; seed < 20; seed++) {
            RunResult r = new Cluster(hard(6000), RaftConfig.correct().withoutNoOp(), seed).run();
            assertTrue(r.safe(), "seed " + seed + ": " + r.violation());
        }
    }

    @Test
    void worksForOneThreeAndSevenNodes() {
        for (int n : new int[] {1, 3, 7}) {
            RunResult r = new Cluster(Scenario.baseline().withNodes(n).withTicks(3000), RaftConfig.correct(), 2).run();
            assertTrue(r.safe(), n + " nodes: " + r.violation());
            assertTrue(r.committedCommands() > 100, n + " nodes committed " + r.committedCommands());
        }
    }

    @Test
    void metricsAreRecorded() {
        RunResult r = new Cluster(hard(5000), RaftConfig.correct(), 4).run();
        assertTrue(r.crashes() > 0 && r.partitions() > 0 && r.electionsStarted() > 1);
        assertTrue(r.messagesSent() > r.messagesDropped() && r.messagesDropped() > 0);
        assertFalse(r.throughputPer1000() < 0);
    }
}
