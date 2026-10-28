package raftsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Drives a whole Raft cluster through logical time with injected faults, and checks safety after every tick.
 * Two runs with the same {@link Scenario}, {@link RaftConfig} and seed behave identically, tick for tick.
 */
public final class Cluster {
    private final Scenario sc;
    private final RaftConfig cfg;
    private final long seed;
    private final Random rng;                 // crashes, partitions (separate stream from the network's)
    private final Network net;
    private final InvariantChecker checker = new InvariantChecker();
    private final PersistentState[] states;
    private final RaftNode[] nodes;           // null = crashed
    private final long[] restartAt;
    private final int[] incarnation;

    private long now;
    private int nextCommand = 1;
    private int accepted;
    private int electionsStarted;
    private int crashes;
    private int partitions;
    private long partitionEnds = -1;

    private boolean everHadLeader;
    private long unavailableTicks;
    private int unavailableGaps;
    private long currentGap;
    private long maxGap;

    public Cluster(Scenario scenario, RaftConfig config, long seed) {
        this.sc = scenario;
        this.cfg = config;
        this.seed = seed;
        this.rng = new Random(seed * 31 + 29);
        this.net = new Network(new Random(seed * 31 + 17), sc.dropProb(), sc.dupProb(), sc.minDelay(), sc.maxDelay());
        this.states = new PersistentState[sc.nodes()];
        this.nodes = new RaftNode[sc.nodes()];
        this.restartAt = new long[sc.nodes()];
        this.incarnation = new int[sc.nodes()];
        for (int i = 0; i < sc.nodes(); i++) {
            states[i] = new PersistentState();
            nodes[i] = newNode(i);
        }
    }

    /** Runs to the end of the scenario, or until the first safety violation. */
    public RunResult run() {
        while (now < sc.ticks() && checker.first() == null) {
            tick();
        }
        return result();
    }

    public void tick() {
        now++;
        applyFaults();
        for (Network.Delivery d : net.due(now)) {
            RaftNode target = nodes[d.to()];
            if (target != null) {
                target.handle(d.from(), d.message());
            }
        }
        for (RaftNode node : nodes) {
            if (node != null) {
                node.tick();
            }
        }
        clientTraffic();
        checker.check(now, nodes);
        trackAvailability();
    }

    // ---- inspection (used by tests) --------------------------------------------------------------------------
    public RaftNode node(int i) { return nodes[i]; }

    public boolean isUp(int i) { return nodes[i] != null; }

    public long now() { return now; }

    public RaftNode leader() { return currentLeader(); }

    public void crash(int i) {
        if (nodes[i] != null) {
            nodes[i] = null; // volatile state is gone; states[i] (term, vote, log) survives
            crashes++;
        }
    }

    public void restart(int i) {
        if (nodes[i] == null) {
            nodes[i] = newNode(i);
            restartAt[i] = 0;
        }
    }

    /** Cut these nodes off from the rest of the cluster (tests use this to stage a partition by hand). */
    public void isolate(Set<Integer> group) { net.isolate(group); }

    public void heal() { net.heal(); }

    public Violation violation() { return checker.first(); }

    // ---- internals -------------------------------------------------------------------------------------------
    private RaftNode newNode(int i) {
        Random nodeRng = new Random(seed * 1_000_003L + i * 7_919L + incarnation[i]++);
        RaftNode.Hooks hooks = new RaftNode.Hooks() {
            @Override
            public void electionStarted() {
                electionsStarted++;
            }

            @Override
            public void becameLeader(RaftNode node) {
                checker.onLeaderElected(node, now);
            }
        };
        return new RaftNode(i, sc.nodes(), states[i], cfg, nodeRng, (to, message) -> net.send(now, i, to, message), hooks);
    }

    private void applyFaults() {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null && restartAt[i] > 0 && now >= restartAt[i]) {
                restart(i);
            }
        }
        if (sc.crashProbPerTick() > 0 && rng.nextDouble() < sc.crashProbPerTick()) {
            List<Integer> live = liveNodeIds();
            if (!live.isEmpty()) {
                RaftNode leader = currentLeader();
                boolean aimAtLeader = leader != null && rng.nextDouble() < sc.leaderCrashBias();
                int victim = aimAtLeader ? leader.id() : live.get(rng.nextInt(live.size()));
                crash(victim);
                restartAt[victim] = now + sc.minDowntime() + rng.nextInt(sc.maxDowntime() - sc.minDowntime() + 1);
            }
        }
        if (sc.partitionEvery() > 0) {
            if (partitionEnds < 0 && now % sc.partitionEvery() == 0) {
                startPartition();
            } else if (partitionEnds >= 0 && now >= partitionEnds) {
                net.heal();
                partitionEnds = -1;
            }
        }
    }

    private void startPartition() {
        Set<Integer> group = new HashSet<>();
        RaftNode leader = currentLeader();
        if (leader != null && rng.nextBoolean()) {
            group.add(leader.id());                     // cut the leader off: the hardest case for availability
        } else {
            List<Integer> ids = new ArrayList<>();
            for (int i = 0; i < sc.nodes(); i++) {
                ids.add(i);
            }
            Collections.shuffle(ids, rng);
            int size = 1 + rng.nextInt(Math.max(1, (sc.nodes() - 1) / 2));
            group.addAll(ids.subList(0, size));         // always a strict minority
        }
        net.isolate(group);
        partitions++;
        partitionEnds = now + sc.partitionLength();
    }

    private void clientTraffic() {
        if (sc.clientEvery() > 0 && now % sc.clientEvery() == 0) {
            RaftNode leader = currentLeader();
            if (leader != null && leader.propose(nextCommand) > 0) {
                accepted++;
                nextCommand++;
            }
        }
    }

    /** The live leader with the highest term (a stale leader on the minority side of a partition has a lower term). */
    private RaftNode currentLeader() {
        RaftNode best = null;
        for (RaftNode node : nodes) {
            if (node != null && node.role() == RaftNode.Role.LEADER
                    && (best == null || node.currentTerm() > best.currentTerm())) {
                best = node;
            }
        }
        return best;
    }

    /** Available = some live leader can reach a majority of the cluster (so it could actually commit). */
    private boolean hasEffectiveLeader() {
        for (RaftNode node : nodes) {
            if (node != null && node.role() == RaftNode.Role.LEADER) {
                int reachable = 0;
                for (int j = 0; j < nodes.length; j++) {
                    if (nodes[j] != null && !net.blocked(node.id(), j)) {
                        reachable++;
                    }
                }
                if (reachable * 2 > sc.nodes()) {
                    return true;
                }
            }
        }
        return false;
    }

    private void trackAvailability() {
        if (hasEffectiveLeader()) {
            everHadLeader = true;
            maxGap = Math.max(maxGap, currentGap);
            currentGap = 0;
        } else if (everHadLeader) {     // the initial election does not count as an outage
            if (currentGap == 0) {
                unavailableGaps++;
            }
            currentGap++;
            unavailableTicks++;
        }
    }

    private List<Integer> liveNodeIds() {
        List<Integer> live = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                live.add(i);
            }
        }
        return live;
    }

    private RunResult result() {
        return new RunResult(now, checker.first(), checker.committedCommands(), accepted, electionsStarted, crashes,
                partitions, unavailableTicks, unavailableGaps, Math.max(maxGap, currentGap), net.sent(), net.dropped());
    }
}
