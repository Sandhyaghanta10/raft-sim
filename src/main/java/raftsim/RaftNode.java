package raftsim;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;

import raftsim.Message.AppendEntries;
import raftsim.Message.AppendEntriesReply;
import raftsim.Message.RequestVote;
import raftsim.Message.RequestVoteReply;

/**
 * One Raft server (leader election + log replication, no membership changes or snapshots).
 *
 * <p>A pure state machine: it never touches a thread, a socket or a wall clock. Time advances only when
 * {@link #tick()} is called and messages leave only through the injected {@code send} callback, which is what
 * lets {@link Cluster} replay any run exactly from its seed.
 *
 * <p>Log indexes are 1-based, as in the paper; index 0 means "before the first entry".
 */
public final class RaftNode {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** Observer for events the simulator needs to count or check. */
    public interface Hooks {
        Hooks NONE = new Hooks() {};

        default void electionStarted() {}

        default void becameLeader(RaftNode node) {}
    }

    private static final int MAX_BATCH = 50;

    private final int id;
    private final int clusterSize;
    private final PersistentState ps;
    private final RaftConfig cfg;
    private final Random rng;
    private final BiConsumer<Integer, Message> send;
    private final Hooks hooks;

    // Volatile state: lost on a crash and rebuilt from the persistent state.
    private Role role = Role.FOLLOWER;
    private int commitIndex;
    private final int[] nextIndex;
    private final int[] matchIndex;
    private Set<Integer> votes = new HashSet<>();
    private int electionElapsed;
    private int electionTimeout;
    private int heartbeatElapsed;

    public RaftNode(int id, int clusterSize, PersistentState ps, RaftConfig cfg, Random rng,
                    BiConsumer<Integer, Message> send, Hooks hooks) {
        this.id = id;
        this.clusterSize = clusterSize;
        this.ps = ps;
        this.cfg = cfg;
        this.rng = rng;
        this.send = send;
        this.hooks = hooks;
        this.nextIndex = new int[clusterSize];
        this.matchIndex = new int[clusterSize];
        if (!cfg.persistVote()) {
            ps.votedFor = -1; // bug mode: the vote was only kept in memory, so a restart forgets it
        }
        resetElectionTimeout();
    }

    // ---- read-only view, used by tests and the invariant checker -------------------------------------------
    public int id() { return id; }

    public Role role() { return role; }

    public int currentTerm() { return ps.currentTerm; }

    public int votedFor() { return ps.votedFor; }

    public int commitIndex() { return commitIndex; }

    public int lastLogIndex() { return ps.log.size(); }

    public LogEntry entryAt(int index) { return ps.log.get(index - 1); }

    public List<LogEntry> log() { return Collections.unmodifiableList(ps.log); }

    // ---- inputs ----------------------------------------------------------------------------------------------
    /** One unit of logical time. */
    public void tick() {
        if (role == Role.LEADER) {
            if (++heartbeatElapsed >= cfg.heartbeatInterval()) {
                broadcastAppend();
            }
        } else if (++electionElapsed >= electionTimeout) {
            startElection();
        }
    }

    /** Client request. Returns the log index it was appended at, or -1 if this node is not the leader. */
    public int propose(int command) {
        if (role != Role.LEADER) {
            return -1;
        }
        ps.log.add(new LogEntry(ps.currentTerm, command));
        advanceCommit(); // only matters for a single-node cluster
        broadcastAppend();
        return lastLogIndex();
    }

    public void handle(int from, Message message) {
        if (message.term() > ps.currentTerm) {
            stepDown(message.term());
        }
        switch (message) {
            case RequestVote rv -> onRequestVote(from, rv);
            case RequestVoteReply reply -> onVoteReply(from, reply);
            case AppendEntries ae -> onAppendEntries(from, ae);
            case AppendEntriesReply reply -> onAppendReply(from, reply);
        }
    }

    // ---- elections -------------------------------------------------------------------------------------------
    private void startElection() {
        role = Role.CANDIDATE;
        ps.currentTerm++;
        ps.votedFor = id;
        votes = new HashSet<>(Set.of(id));
        resetElectionTimeout();
        hooks.electionStarted();
        if (hasMajority(votes.size())) {
            becomeLeader();
            return;
        }
        RequestVote request = new RequestVote(ps.currentTerm, id, lastLogIndex(), termAt(lastLogIndex()));
        for (int peer = 0; peer < clusterSize; peer++) {
            if (peer != id) {
                send.accept(peer, request);
            }
        }
    }

    private void onRequestVote(int from, RequestVote rv) {
        boolean granted = rv.term() == ps.currentTerm
                && (ps.votedFor == -1 || ps.votedFor == rv.candidateId())
                && (!cfg.checkLogUpToDate() || candidateLogIsUpToDate(rv));
        if (granted) {
            ps.votedFor = rv.candidateId();
            resetElectionTimeout();
        }
        send.accept(from, new RequestVoteReply(ps.currentTerm, granted));
    }

    private boolean candidateLogIsUpToDate(RequestVote rv) {
        int myLastTerm = termAt(lastLogIndex());
        return rv.lastLogTerm() > myLastTerm
                || (rv.lastLogTerm() == myLastTerm && rv.lastLogIndex() >= lastLogIndex());
    }

    private void onVoteReply(int from, RequestVoteReply reply) {
        if (role == Role.CANDIDATE && reply.term() == ps.currentTerm && reply.granted()) {
            votes.add(from); // a Set, so duplicated replies cannot be double-counted
            if (hasMajority(votes.size())) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        Arrays.fill(nextIndex, lastLogIndex() + 1);
        Arrays.fill(matchIndex, 0);
        if (cfg.appendNoOp()) {
            ps.log.add(new LogEntry(ps.currentTerm, LogEntry.NOOP)); // lets entries from older terms commit promptly
        }
        hooks.becameLeader(this);
        advanceCommit();
        broadcastAppend();
    }

    private void stepDown(int newTerm) {
        ps.currentTerm = newTerm;
        ps.votedFor = -1;
        role = Role.FOLLOWER;
        resetElectionTimeout();
    }

    // ---- replication -----------------------------------------------------------------------------------------
    private void broadcastAppend() {
        heartbeatElapsed = 0;
        for (int peer = 0; peer < clusterSize; peer++) {
            if (peer != id) {
                sendAppend(peer);
            }
        }
    }

    private void sendAppend(int peer) {
        int prev = nextIndex[peer] - 1;
        int last = Math.min(lastLogIndex(), prev + MAX_BATCH);
        List<LogEntry> entries = List.copyOf(ps.log.subList(prev, last));
        send.accept(peer, new AppendEntries(ps.currentTerm, id, prev, termAt(prev), entries, commitIndex));
    }

    private void onAppendEntries(int from, AppendEntries ae) {
        if (ae.term() < ps.currentTerm) {
            send.accept(from, new AppendEntriesReply(ps.currentTerm, false, 0, lastLogIndex()));
            return;
        }
        role = Role.FOLLOWER; // a candidate that hears from a legitimate leader gives up
        resetElectionTimeout();

        boolean prevMatches = ae.prevLogIndex() <= lastLogIndex()
                && (ae.prevLogIndex() == 0 || termAt(ae.prevLogIndex()) == ae.prevLogTerm());
        if (!prevMatches) {
            send.accept(from, new AppendEntriesReply(ps.currentTerm, false, 0, lastLogIndex()));
            return;
        }
        int index = ae.prevLogIndex();
        for (LogEntry entry : ae.entries()) {
            index++;
            if (index <= lastLogIndex() && termAt(index) != entry.term()) {
                truncateFrom(index); // conflicting suffix: the leader's log wins
            }
            if (index > lastLogIndex()) {
                ps.log.add(entry);
            }
        }
        int lastNew = ae.prevLogIndex() + ae.entries().size();
        if (ae.leaderCommit() > commitIndex) {
            commitIndex = Math.min(ae.leaderCommit(), lastNew);
        }
        send.accept(from, new AppendEntriesReply(ps.currentTerm, true, lastNew, lastLogIndex()));
    }

    private void onAppendReply(int from, AppendEntriesReply reply) {
        if (role != Role.LEADER || reply.term() != ps.currentTerm) {
            return; // stale reply from an old term, or we are no longer leader
        }
        if (reply.success()) {
            matchIndex[from] = Math.max(matchIndex[from], reply.matchIndex());
            nextIndex[from] = matchIndex[from] + 1;
            advanceCommit();
            if (nextIndex[from] <= lastLogIndex()) {
                sendAppend(from); // keep streaming without waiting for the next heartbeat
            }
        } else {
            nextIndex[from] = Math.max(matchIndex[from] + 1, Math.min(nextIndex[from] - 1, reply.lastLogIndex() + 1));
            sendAppend(from);
        }
    }

    private void advanceCommit() {
        for (int n = lastLogIndex(); n > commitIndex; n--) {
            if (cfg.commitOnlyCurrentTerm() && termAt(n) != ps.currentTerm) {
                break; // Figure 8: never count replicas for an older term's entry
            }
            int replicas = 1; // this leader
            for (int peer = 0; peer < clusterSize; peer++) {
                if (peer != id && matchIndex[peer] >= n) {
                    replicas++;
                }
            }
            if (hasMajority(replicas)) {
                commitIndex = n;
                break;
            }
        }
    }

    // ---- helpers ---------------------------------------------------------------------------------------------
    private boolean hasMajority(int count) {
        return count > clusterSize / 2;
    }

    private int termAt(int index) {
        return index == 0 ? 0 : ps.log.get(index - 1).term();
    }

    private void truncateFrom(int index) {
        while (ps.log.size() >= index) {
            ps.log.remove(ps.log.size() - 1);
        }
    }

    private void resetElectionTimeout() {
        electionElapsed = 0;
        electionTimeout = cfg.electionTimeoutMin()
                + rng.nextInt(cfg.electionTimeoutMax() - cfg.electionTimeoutMin() + 1);
    }

    @Override
    public String toString() {
        return "Node" + id + "[" + role + " term=" + ps.currentTerm + " log=" + ps.log.size() + " commit=" + commitIndex + "]";
    }
}
