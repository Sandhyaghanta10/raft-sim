package raftsim;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 * A lossy, reordering network driven by logical time.
 *
 * <p>Every message gets a random delivery time, so messages overtake each other (reordering). Messages can be
 * dropped or duplicated, and a set of nodes can be cut off from everyone else (a partition). All randomness comes
 * from one seeded {@link Random}, so a run is exactly reproducible.
 */
public final class Network {
    public record Delivery(long deliverAt, long seq, int from, int to, Message message) {}

    private final PriorityQueue<Delivery> queue = new PriorityQueue<>(
            Comparator.comparingLong(Delivery::deliverAt).thenComparingLong(Delivery::seq));
    private final Random rng;
    private final double dropProb;
    private final double dupProb;
    private final int minDelay;
    private final int maxDelay;
    private Set<Integer> isolated = Set.of();
    private long seq;
    private long sent;
    private long dropped;

    public Network(Random rng, double dropProb, double dupProb, int minDelay, int maxDelay) {
        if (minDelay < 1 || maxDelay < minDelay) {
            throw new IllegalArgumentException("delays must satisfy 1 <= min <= max");
        }
        this.rng = rng;
        this.dropProb = dropProb;
        this.dupProb = dupProb;
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
    }

    public void send(long now, int from, int to, Message message) {
        sent++;
        if (blocked(from, to) || rng.nextDouble() < dropProb) {
            dropped++;
            return;
        }
        schedule(now, from, to, message);
        if (rng.nextDouble() < dupProb) {
            schedule(now, from, to, message); // the same message arrives a second time, at a different moment
        }
    }

    private void schedule(long now, int from, int to, Message message) {
        long delay = minDelay + rng.nextInt(maxDelay - minDelay + 1);
        queue.add(new Delivery(now + delay, seq++, from, to, message));
    }

    /** Messages due at or before {@code now}, oldest first. Messages crossing an active partition are lost. */
    public List<Delivery> due(long now) {
        List<Delivery> out = new ArrayList<>();
        while (!queue.isEmpty() && queue.peek().deliverAt() <= now) {
            Delivery d = queue.poll();
            if (blocked(d.from(), d.to())) {
                dropped++;
            } else {
                out.add(d);
            }
        }
        return out;
    }

    /** Cut {@code group} off from every node outside it (nodes inside the group still talk to each other). */
    public void isolate(Set<Integer> group) {
        isolated = Set.copyOf(group);
    }

    public void heal() {
        isolated = Set.of();
    }

    public boolean blocked(int a, int b) {
        return isolated.contains(a) != isolated.contains(b);
    }

    public long sent() { return sent; }

    public long dropped() { return dropped; }
}
