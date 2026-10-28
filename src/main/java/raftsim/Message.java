package raftsim;

import java.util.List;

/** The four Raft RPCs. All payloads are immutable, so a duplicated message is safe to share. */
public sealed interface Message {
    int term();

    record RequestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm) implements Message {}

    record RequestVoteReply(int term, boolean granted) implements Message {}

    record AppendEntries(int term, int leaderId, int prevLogIndex, int prevLogTerm,
                         List<LogEntry> entries, int leaderCommit) implements Message {}

    /** {@code matchIndex} is absolute (highest index known replicated), so duplicate or reordered replies are harmless. */
    record AppendEntriesReply(int term, boolean success, int matchIndex, int lastLogIndex) implements Message {}
}
