package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import raftsim.Message.AppendEntries;

/** The checker must be able to FAIL. These tests hand it broken states and expect each violation to be reported. */
class InvariantCheckerTest {
    private final InvariantChecker checker = new InvariantChecker();

    private static RaftNode node(int id, int clusterSize, PersistentState ps) {
        return new RaftNode(id, clusterSize, ps, RaftConfig.correct(), new Random(id), (to, m) -> {}, RaftNode.Hooks.NONE);
    }

    private static RaftNode soleLeader(int id, int startingTerm) {
        PersistentState ps = new PersistentState();
        ps.currentTerm = startingTerm;
        RaftNode n = node(id, 1, ps);
        while (n.role() != RaftNode.Role.LEADER) {
            n.tick();
        }
        return n;
    }

    private static RaftNode followerCommitting(int id, LogEntry entry) {
        RaftNode n = node(id, 3, new PersistentState());
        n.handle(0, new AppendEntries(1, 0, 0, 0, List.of(entry), 1));
        assertEquals(1, n.commitIndex());
        return n;
    }

    @Test
    void healthyStatesPass() {
        RaftNode a = followerCommitting(0, new LogEntry(1, 5));
        RaftNode b = followerCommitting(1, new LogEntry(1, 5));
        checker.check(10, new RaftNode[] {a, b, null});
        assertNull(checker.first());
        assertEquals(1, checker.committedCommands());
    }

    @Test
    void detectsTwoLeadersInTheSameTerm() {
        checker.onLeaderElected(soleLeader(0, 0), 5);
        checker.onLeaderElected(soleLeader(1, 0), 6);
        assertNotNull(checker.first());
        assertEquals(Violation.ELECTION_SAFETY, checker.first().kind());
    }

    @Test
    void sameNodeWinningTheSameTermTwiceIsNotAViolation() {
        RaftNode leader = soleLeader(0, 0);
        checker.onLeaderElected(leader, 5);
        checker.onLeaderElected(leader, 6);
        assertNull(checker.first());
    }

    @Test
    void detectsDifferentCommandsCommittedAtTheSameIndex() {
        RaftNode a = followerCommitting(0, new LogEntry(1, 5));
        RaftNode b = followerCommitting(1, new LogEntry(1, 6));
        checker.check(1, new RaftNode[] {a, b});
        assertEquals(Violation.STATE_MACHINE_SAFETY, checker.first().kind());
    }

    @Test
    void detectsALaterLeaderThatLacksACommittedEntry() {
        RaftNode first = soleLeader(0, 0);                 // term 1, log [(1,noop)], committed
        checker.check(1, new RaftNode[] {first});
        assertNull(checker.first());
        RaftNode later = soleLeader(1, 4);                 // term 5 leader with a completely different log
        checker.onLeaderElected(later, 2);
        assertEquals(Violation.LEADER_COMPLETENESS, checker.first().kind());
    }

    @Test
    void detectsLogsThatAgreeOnATermButDifferBefore() {
        PersistentState x = new PersistentState();
        x.log.add(new LogEntry(1, 1));
        x.log.add(new LogEntry(2, 2));
        PersistentState y = new PersistentState();
        y.log.add(new LogEntry(1, 9));                     // same term, different command
        y.log.add(new LogEntry(2, 2));
        checker.check(10, new RaftNode[] {node(0, 3, x), node(1, 3, y)});
        assertEquals(Violation.LOG_MATCHING, checker.first().kind());
    }

    @Test
    void remembersOnlyTheFirstViolation() {
        checker.onLeaderElected(soleLeader(0, 0), 1);
        checker.onLeaderElected(soleLeader(1, 0), 2);
        long firstTick = checker.first().tick();
        checker.onLeaderElected(soleLeader(2, 0), 9);
        assertEquals(firstTick, checker.first().tick());
    }
}
