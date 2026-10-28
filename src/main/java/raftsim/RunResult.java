package raftsim;

/** Outcome of one simulated run. {@code violation} is null when every safety property held. */
public record RunResult(long ticks, Violation violation, int committedCommands, int acceptedProposals,
                        int electionsStarted, int crashes, int partitions,
                        long unavailableTicks, int unavailableGaps, long maxUnavailableGap,
                        long messagesSent, long messagesDropped) {

    public boolean safe() {
        return violation == null;
    }

    /** Client commands committed per 1000 ticks. */
    public double throughputPer1000() {
        return ticks == 0 ? 0 : committedCommands * 1000.0 / ticks;
    }

    public double unavailableFraction() {
        return ticks == 0 ? 0 : (double) unavailableTicks / ticks;
    }
}
