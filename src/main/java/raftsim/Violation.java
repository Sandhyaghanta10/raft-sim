package raftsim;

/** A broken safety property, with the logical time and a human-readable description. */
public record Violation(String kind, long tick, String detail) {
    public static final String ELECTION_SAFETY = "ElectionSafety";
    public static final String LEADER_COMPLETENESS = "LeaderCompleteness";
    public static final String STATE_MACHINE_SAFETY = "StateMachineSafety";
    public static final String LOG_MATCHING = "LogMatching";

    @Override
    public String toString() {
        return kind + " @tick " + tick + ": " + detail;
    }
}
