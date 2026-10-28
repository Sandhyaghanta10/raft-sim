package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import raftsim.Message.RequestVoteReply;

class NetworkTest {
    private static final Message MSG = new RequestVoteReply(1, true);

    private Network network(double drop, double dup) {
        return new Network(new Random(7), drop, dup, 2, 6);
    }

    @Test
    void deliversWithinTheDelayWindow() {
        Network net = network(0, 0);
        for (int i = 0; i < 50; i++) {
            net.send(100, 0, 1, MSG);
        }
        assertTrue(net.due(101).isEmpty(), "minimum delay is 2 ticks");
        assertEquals(50, net.due(106).size(), "maximum delay is 6 ticks");
    }

    @Test
    void messagesCanOvertakeEachOther() {
        Network net = network(0, 0);
        for (int i = 0; i < 40; i++) {
            net.send(0, i, 1, MSG);
        }
        List<Network.Delivery> delivered = net.due(10);
        boolean reordered = false;
        for (int i = 1; i < delivered.size(); i++) {
            reordered |= delivered.get(i).from() < delivered.get(i - 1).from();
        }
        assertTrue(reordered, "with random delays, some later message should arrive first");
    }

    @Test
    void dropProbabilityOneLosesEverythingAndCountsIt() {
        Network net = network(1.0, 0);
        net.send(0, 0, 1, MSG);
        assertTrue(net.due(100).isEmpty());
        assertEquals(1, net.sent());
        assertEquals(1, net.dropped());
    }

    @Test
    void duplicateProbabilityOneDeliversEverythingTwice() {
        Network net = network(0, 1.0);
        net.send(0, 0, 1, MSG);
        assertEquals(2, net.due(100).size());
    }

    @Test
    void partitionBlocksTrafficAcrossTheCutButNotInsideIt() {
        Network net = network(0, 0);
        net.isolate(Set.of(0, 1));
        assertTrue(net.blocked(0, 2));
        assertTrue(net.blocked(2, 1));
        assertFalse(net.blocked(0, 1));
        assertFalse(net.blocked(2, 3));
        net.send(0, 0, 2, MSG);
        net.send(0, 0, 1, MSG);
        assertEquals(1, net.due(100).size());
        net.heal();
        assertFalse(net.blocked(0, 2));
    }

    @Test
    void messagesAlreadyInFlightWhenAPartitionStartsAreLost() {
        Network net = network(0, 0);
        net.send(0, 0, 2, MSG);
        net.isolate(Set.of(0));
        assertTrue(net.due(100).isEmpty());
        assertEquals(1, net.dropped());
    }

    @Test
    void rejectsImpossibleDelays() {
        assertThrows(IllegalArgumentException.class, () -> new Network(new Random(1), 0, 0, 0, 5));
        assertThrows(IllegalArgumentException.class, () -> new Network(new Random(1), 0, 0, 5, 3));
    }

    @Test
    void sameSeedGivesTheSameDeliverySchedule() {
        Network a = network(0.3, 0.2);
        Network b = network(0.3, 0.2);
        for (int i = 0; i < 100; i++) {
            a.send(i, i % 5, (i + 1) % 5, MSG);
            b.send(i, i % 5, (i + 1) % 5, MSG);
        }
        assertEquals(a.due(1000), b.due(1000));
    }
}
