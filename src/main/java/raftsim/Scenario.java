package raftsim;

/**
 * Everything about one simulated run except the Raft configuration and the seed.
 *
 * @param crashProbPerTick  chance per tick that one live node crashes (it restarts after a random downtime)
 * @param partitionEvery    every N ticks, isolate a minority (or the current leader) for {@code partitionLength}; 0 = never
 * @param clientEvery       a client submits one command to the leader every N ticks; 0 = no client load
 * @param leaderCrashBias   when a crash happens, the chance that its victim is the current leader instead of a random node
 */
public record Scenario(int nodes, long ticks, double dropProb, double dupProb, int minDelay, int maxDelay,
                       double crashProbPerTick, int minDowntime, int maxDowntime,
                       int partitionEvery, int partitionLength, int clientEvery, double leaderCrashBias) {

    public static Scenario baseline() {
        return new Scenario(5, 10_000, 0.0, 0.0, 1, 10, 0.0, 50, 200, 0, 0, 20, 0.0);
    }

    public Scenario withDrop(double drop) {
        return new Scenario(nodes, ticks, drop, dupProb, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }

    public Scenario withDuplicates(double dup) {
        return new Scenario(nodes, ticks, dropProb, dup, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }

    public Scenario withCrashes(double crashProb) {
        return new Scenario(nodes, ticks, dropProb, dupProb, minDelay, maxDelay, crashProb, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }

    public Scenario withDowntime(int min, int max) {
        return new Scenario(nodes, ticks, dropProb, dupProb, minDelay, maxDelay, crashProbPerTick, min, max,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }

    public Scenario withLeaderCrashBias(double bias) {
        return new Scenario(nodes, ticks, dropProb, dupProb, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, bias);
    }

    public Scenario withPartitions(int every, int length) {
        return new Scenario(nodes, ticks, dropProb, dupProb, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                every, length, clientEvery, leaderCrashBias);
    }

    public Scenario withTicks(long newTicks) {
        return new Scenario(nodes, newTicks, dropProb, dupProb, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }

    public Scenario withNodes(int n) {
        return new Scenario(n, ticks, dropProb, dupProb, minDelay, maxDelay, crashProbPerTick, minDowntime, maxDowntime,
                partitionEvery, partitionLength, clientEvery, leaderCrashBias);
    }
}
