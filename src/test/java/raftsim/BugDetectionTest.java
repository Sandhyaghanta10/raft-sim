package raftsim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Regression tests for the experiment's claim "the checker catches these bugs". The seeds are the first ones the
 * experiment found (see results/results.md); a run is deterministic, so each seed reproduces the same failure forever.
 * If someone changes the protocol or the simulator and these stop failing under the buggy rule, detection has regressed.
 */
class BugDetectionTest {
    private static final long TICKS = 10_000;

    private static RunResult run(RaftConfig cfg, long seed) {
        return new Cluster(Experiment.hard(TICKS), cfg, seed).run();
    }

    @Test
    void skippingTheLogUpToDateCheckBreaksLeaderCompleteness() {
        RunResult r = run(RaftConfig.correct().withoutLogUpToDateCheck(), 0);
        assertNotNull(r.violation(), "seed 0 should expose the missing election restriction");
        assertEquals(Violation.LEADER_COMPLETENESS, r.violation().kind());
    }

    @Test
    void forgettingTheVoteAfterARestartLetsTwoLeadersWinTheSameTerm() {
        RunResult r = run(RaftConfig.correct().withoutVotePersistence(), 3);
        assertNotNull(r.violation(), "seed 3 should expose the double vote");
        assertEquals(Violation.ELECTION_SAFETY, r.violation().kind());
    }

    @Test
    void theCorrectProtocolSurvivesTheSameSeeds() {
        for (long seed = 0; seed < 10; seed++) {
            RunResult r = run(RaftConfig.correct(), seed);
            assertTrue(r.safe(), "seed " + seed + ": " + r.violation());
        }
    }

    @Test
    void aViolationStopsTheRunEarly() {
        RunResult r = run(RaftConfig.correct().withoutLogUpToDateCheck(), 0);
        assertTrue(r.ticks() < TICKS, "the run stops at the first violation instead of simulating on");
        assertEquals(r.violation().tick(), r.ticks());
    }
}
