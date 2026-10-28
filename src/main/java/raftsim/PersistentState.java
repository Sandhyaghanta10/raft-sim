package raftsim;

import java.util.ArrayList;
import java.util.List;

/**
 * The state Raft requires to survive a crash (Figure 2 of the paper): current term, vote, and log.
 * The simulator keeps one of these per node across crash/restart; everything else in {@link RaftNode} is volatile.
 */
public final class PersistentState {
    int currentTerm;
    int votedFor = -1;
    final List<LogEntry> log = new ArrayList<>();
}
