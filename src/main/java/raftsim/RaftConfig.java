package raftsim;

/**
 * Raft timing plus switches for safety rules. The defaults are the correct protocol; turning a rule off
 * re-creates a classic implementation bug so we can check that the invariant checker really catches it.
 *
 * @param checkLogUpToDate      voters reject candidates whose log is behind theirs (election restriction, section 5.4.1)
 * @param persistVote           the vote survives a crash (votedFor is persistent state)
 * @param commitOnlyCurrentTerm a leader counts replicas only for entries of its own term (section 5.4.2, Figure 8)
 * @param appendNoOp            a new leader appends a no-op entry, so entries from older terms can commit promptly
 */
public record RaftConfig(int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval,
                         boolean checkLogUpToDate, boolean persistVote, boolean commitOnlyCurrentTerm,
                         boolean appendNoOp) {

    public static RaftConfig correct() {
        return new RaftConfig(150, 300, 50, true, true, true, true);
    }

    public RaftConfig withElectionTimeout(int min, int max) {
        return new RaftConfig(min, max, heartbeatInterval, checkLogUpToDate, persistVote, commitOnlyCurrentTerm, appendNoOp);
    }

    public RaftConfig withoutLogUpToDateCheck() {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, false, persistVote, commitOnlyCurrentTerm, appendNoOp);
    }

    public RaftConfig withoutVotePersistence() {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, checkLogUpToDate, false, commitOnlyCurrentTerm, appendNoOp);
    }

    public RaftConfig withCommitOfOldTerms() {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, checkLogUpToDate, persistVote, false, appendNoOp);
    }

    public RaftConfig withoutNoOp() {
        return new RaftConfig(electionTimeoutMin, electionTimeoutMax, heartbeatInterval, checkLogUpToDate, persistVote, commitOnlyCurrentTerm, false);
    }
}
