package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import raftsim.Message.AppendEntries;
import raftsim.Message.AppendEntriesReply;
import raftsim.Message.RequestVote;
import raftsim.Message.RequestVoteReply;

/** Rule-by-rule tests of a single node, with its outgoing messages captured instead of delivered. */
class RaftNodeTest {
    private record Sent(int to, Message message) {}

    private final List<Sent> sent = new ArrayList<>();
    private final PersistentState state = new PersistentState();

    private RaftNode node(int id, int clusterSize, RaftConfig cfg) {
        return new RaftNode(id, clusterSize, state, cfg, new Random(1), (to, m) -> sent.add(new Sent(to, m)), RaftNode.Hooks.NONE);
    }

    private <T extends Message> T lastSent(Class<T> type) {
        for (int i = sent.size() - 1; i >= 0; i--) {
            if (type.isInstance(sent.get(i).message())) {
                return type.cast(sent.get(i).message());
            }
        }
        throw new AssertionError("no " + type.getSimpleName() + " was sent");
    }

    private void logOf(int... terms) {
        for (int term : terms) {
            state.log.add(new LogEntry(term, state.log.size() + 1));
        }
    }

    // ---- voting ------------------------------------------------------------------------------------------------
    @Test
    void votesForAtMostOneCandidatePerTerm() {
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new RequestVote(1, 1, 0, 0));
        assertTrue(lastSent(RequestVoteReply.class).granted());
        n.handle(2, new RequestVote(1, 2, 0, 0));
        assertFalse(lastSent(RequestVoteReply.class).granted(), "second candidate in the same term must be refused");
        n.handle(1, new RequestVote(1, 1, 0, 0));
        assertTrue(lastSent(RequestVoteReply.class).granted(), "a retransmitted request from the same candidate is fine");
    }

    @Test
    void refusesCandidateWhoseLogIsBehind() {
        state.currentTerm = 2;
        logOf(1, 2);
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new RequestVote(3, 1, 5, 1));   // longer log but older last term
        assertFalse(lastSent(RequestVoteReply.class).granted());
        n.handle(2, new RequestVote(4, 2, 1, 2));   // same last term but shorter log
        assertFalse(lastSent(RequestVoteReply.class).granted());
        n.handle(2, new RequestVote(5, 2, 2, 2));   // same last term, same length
        assertTrue(lastSent(RequestVoteReply.class).granted());
    }

    @Test
    void bugModeSkippingTheLogCheckWouldGrantTheStaleCandidate() {
        state.currentTerm = 2;
        logOf(1, 2);
        RaftNode n = node(0, 3, RaftConfig.correct().withoutLogUpToDateCheck());
        n.handle(1, new RequestVote(3, 1, 0, 0));
        assertTrue(lastSent(RequestVoteReply.class).granted());
    }

    @Test
    void rejectsRequestsFromOldTermsAndAdoptsNewerTerms() {
        state.currentTerm = 5;
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new RequestVote(4, 1, 0, 0));
        assertFalse(lastSent(RequestVoteReply.class).granted());
        assertEquals(5, lastSent(RequestVoteReply.class).term());
        n.handle(1, new RequestVote(9, 1, 0, 0));
        assertEquals(9, n.currentTerm());
        assertTrue(lastSent(RequestVoteReply.class).granted());
    }

    @Test
    void voteSurvivesRestartUnlessPersistenceBugIsEnabled() {
        RaftNode first = node(0, 3, RaftConfig.correct());
        first.handle(1, new RequestVote(1, 1, 0, 0));
        assertEquals(1, node(0, 3, RaftConfig.correct()).votedFor(), "restart keeps the persisted vote");
        assertEquals(-1, node(0, 3, RaftConfig.correct().withoutVotePersistence()).votedFor());
    }

    @Test
    void candidateBecomesLeaderOnMajorityAndCountsEachVoterOnce() {
        RaftNode n = node(0, 5, RaftConfig.correct());
        while (n.role() != RaftNode.Role.CANDIDATE) {
            n.tick();
        }
        int term = n.currentTerm();
        n.handle(1, new RequestVoteReply(term, true));
        n.handle(1, new RequestVoteReply(term, true));   // duplicate must not count twice
        assertEquals(RaftNode.Role.CANDIDATE, n.role(), "2 of 5 votes is not a majority");
        n.handle(2, new RequestVoteReply(term, true));
        assertEquals(RaftNode.Role.LEADER, n.role());
        assertEquals(LogEntry.NOOP, n.entryAt(n.lastLogIndex()).command(), "a new leader appends a no-op");
    }

    // ---- log replication ---------------------------------------------------------------------------------------
    @Test
    void appendEntriesTruncatesOnlyConflictingSuffix() {
        state.currentTerm = 1;
        logOf(1, 1, 1);
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new AppendEntries(2, 1, 1, 1, List.of(new LogEntry(2, 99)), 0));
        assertEquals(List.of(new LogEntry(1, 1), new LogEntry(2, 99)), n.log());
        assertTrue(lastSent(AppendEntriesReply.class).success());
        assertEquals(2, lastSent(AppendEntriesReply.class).matchIndex());

        n.handle(1, new AppendEntries(2, 1, 1, 1, List.of(new LogEntry(2, 99)), 0));   // duplicate
        assertEquals(2, n.lastLogIndex());
        n.handle(1, new AppendEntries(2, 1, 0, 0, List.of(new LogEntry(1, 1)), 0));    // stale, older, reordered
        assertEquals(2, n.lastLogIndex(), "a delayed old message must not shorten the log");
    }

    @Test
    void appendEntriesRejectsWhenPreviousEntryDoesNotMatch() {
        state.currentTerm = 1;
        logOf(1);
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new AppendEntries(1, 1, 3, 1, List.of(), 0));      // gap in the log
        assertFalse(lastSent(AppendEntriesReply.class).success());
        assertEquals(1, lastSent(AppendEntriesReply.class).lastLogIndex(), "reply tells the leader where to back up to");
        n.handle(1, new AppendEntries(1, 1, 1, 2, List.of(), 0));      // right index, wrong term
        assertFalse(lastSent(AppendEntriesReply.class).success());
    }

    @Test
    void appendEntriesFromAnOldTermIsRefused() {
        state.currentTerm = 5;
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new AppendEntries(4, 1, 0, 0, List.of(new LogEntry(4, 1)), 0));
        assertFalse(lastSent(AppendEntriesReply.class).success());
        assertEquals(5, lastSent(AppendEntriesReply.class).term());
        assertEquals(0, n.lastLogIndex());
    }

    @Test
    void followerCommitIndexNeverPassesTheLastEntryTheLeaderSent() {
        state.currentTerm = 1;
        logOf(1, 1);
        RaftNode n = node(0, 3, RaftConfig.correct());
        n.handle(1, new AppendEntries(1, 1, 0, 0, List.of(new LogEntry(1, 1)), 10));
        assertEquals(1, n.commitIndex());
    }

    // ---- commit rule (Figure 8) --------------------------------------------------------------------------------
    private RaftNode leaderOfTermThreeWithOneOldEntry(RaftConfig cfg) {
        state.currentTerm = 2;
        state.log.add(new LogEntry(2, 7));                 // entry from an older term
        RaftNode n = node(0, 3, cfg);
        while (n.role() != RaftNode.Role.CANDIDATE) {
            n.tick();
        }
        n.handle(1, new RequestVoteReply(n.currentTerm(), true));
        assertEquals(RaftNode.Role.LEADER, n.role());
        assertEquals(3, n.currentTerm());
        return n;
    }

    @Test
    void leaderDoesNotCommitAnOldTermEntryByCountingReplicas() {
        RaftNode leader = leaderOfTermThreeWithOneOldEntry(RaftConfig.correct());
        leader.handle(1, new AppendEntriesReply(3, true, 1, 1));   // peer has only the old entry
        assertEquals(0, leader.commitIndex(), "old-term entry on a majority is still not safe to commit");
        leader.handle(1, new AppendEntriesReply(3, true, 2, 2));   // peer now has the new-term no-op too
        assertEquals(2, leader.commitIndex(), "committing the current-term entry commits everything before it");
    }

    @Test
    void bugModeCommitsTheOldTermEntryEarly() {
        RaftNode leader = leaderOfTermThreeWithOneOldEntry(RaftConfig.correct().withCommitOfOldTerms());
        leader.handle(1, new AppendEntriesReply(3, true, 1, 1));
        assertEquals(1, leader.commitIndex());
    }

    @Test
    void staleReplyFromAnEarlierTermIsIgnored() {
        RaftNode leader = leaderOfTermThreeWithOneOldEntry(RaftConfig.correct());
        leader.handle(1, new AppendEntriesReply(2, true, 2, 2));
        assertEquals(0, leader.commitIndex());
    }

    @Test
    void singleNodeClusterElectsItselfAndCommitsImmediately() {
        RaftNode n = node(0, 1, RaftConfig.correct());
        while (n.role() != RaftNode.Role.LEADER) {
            n.tick();
        }
        assertEquals(2, n.propose(5));
        assertEquals(2, n.commitIndex());
    }

    @Test
    void onlyTheLeaderAcceptsClientCommands() {
        assertEquals(-1, node(0, 3, RaftConfig.correct()).propose(1));
    }

    @Test
    void leaderStepsDownWhenItSeesAHigherTerm() {
        RaftNode leader = leaderOfTermThreeWithOneOldEntry(RaftConfig.correct());
        leader.handle(2, new RequestVote(9, 2, 0, 0));
        assertEquals(RaftNode.Role.FOLLOWER, leader.role());
        assertEquals(9, leader.currentTerm());
    }
}
