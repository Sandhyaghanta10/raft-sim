package raftsim;

/** One replicated-log entry. Command {@link #NOOP} is appended by every new leader to commit older entries safely. */
public record LogEntry(int term, int command) {
    public static final int NOOP = 0;
}
