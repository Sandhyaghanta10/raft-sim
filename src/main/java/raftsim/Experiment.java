package raftsim;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Runs the three experiments and writes a markdown report.
 *
 * <pre>
 *   mvn -q package -DskipTests
 *   java -cp target/classes raftsim.Experiment --seeds 50 --ticks 10000 --bug-seeds 1000 --out results/results.md
 * </pre>
 *
 * Every number comes from the simulator; nothing is hand-edited. Same arguments in, same report out.
 */
public final class Experiment {
    private static final String HEADER = """
            | %s | Safety violations | Commands committed / 1000 ticks | Elections / run | Time with no usable leader | Mean outage (ticks) | Longest outage (ticks) |
            |---|---:|---:|---:|---:|---:|---:|
            """;

    /** Hard scenario used to hunt for bugs: heavy loss, duplicates, frequent crashes and partitions. */
    static Scenario hard(long ticks) {
        return Scenario.baseline().withTicks(ticks).withDrop(0.10).withDuplicates(0.05)
                .withCrashes(0.02).withPartitions(400, 150);
    }

    public static void main(String[] args) throws IOException {
        int seeds = intArg(args, "--seeds", 50);
        int bugSeeds = intArg(args, "--bug-seeds", 1000);
        long ticks = intArg(args, "--ticks", 10_000);
        Path out = Path.of(stringArg(args, "--out", "results/results.md"));

        StringBuilder md = new StringBuilder();
        md.append(String.format(Locale.ROOT, "5-node cluster, %d seeds per row, %d logical ticks per run "
                + "(election timeout 150-300, heartbeat 50, message delay 1-10, one client command every 20 ticks).%n%n", seeds, ticks));

        md.append("### 1. Message loss\n\nNo crashes, no partitions. 2% of messages are also duplicated.\n\n");
        md.append(String.format(HEADER, "Message loss"));
        for (double drop : new double[] {0.0, 0.05, 0.10, 0.20, 0.30, 0.40}) {
            Scenario sc = Scenario.baseline().withTicks(ticks).withDrop(drop).withDuplicates(0.02);
            md.append(row(String.format(Locale.ROOT, "%.0f%%", drop * 100), sc, RaftConfig.correct(), seeds));
        }

        md.append("\n### 2. Node crashes\n\n5% message loss, 2% duplicates. Crash rate is the chance per tick that one random live node "
                + "crashes; it restarts after 50-200 ticks with only its persistent state (term, vote, log).\n\n");
        md.append(String.format(HEADER, "Crash rate / tick"));
        for (double crash : new double[] {0.0, 0.0005, 0.001, 0.002, 0.005}) {
            Scenario sc = Scenario.baseline().withTicks(ticks).withDrop(0.05).withDuplicates(0.02).withCrashes(crash);
            md.append(row(String.format(Locale.ROOT, "%.4f", crash), sc, RaftConfig.correct(), seeds));
        }

        md.append(String.format(Locale.ROOT, "%n### 3. Does the checker catch real bugs?%n%nHard scenario (10%% loss, 5%% duplicates, crash rate 0.02, "
                + "a partition every 400 ticks lasting 150), %d seeds per variant. Each variant disables one safety rule of Raft.%n%n", bugSeeds));
        md.append(bugTable(bugSeeds, ticks));

        String report = md.toString();
        System.out.println(report);
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.writeString(out, report);
    }

    private static String row(String label, Scenario sc, RaftConfig cfg, int seeds) {
        int violations = 0;
        double throughput = 0;
        double elections = 0;
        double unavailable = 0;
        long outageTicks = 0;
        long gaps = 0;
        long longest = 0;
        for (int seed = 0; seed < seeds; seed++) {
            RunResult r = new Cluster(sc, cfg, seed).run();
            violations += r.safe() ? 0 : 1;
            throughput += r.throughputPer1000();
            elections += r.electionsStarted();
            unavailable += r.unavailableFraction();
            outageTicks += r.unavailableTicks();
            gaps += r.unavailableGaps();
            longest = Math.max(longest, r.maxUnavailableGap());
        }
        return String.format(Locale.ROOT, "| %s | %d / %d | %.1f | %.1f | %.2f%% | %s | %d |%n", label, violations, seeds,
                throughput / seeds, elections / seeds, 100 * unavailable / seeds,
                gaps == 0 ? "-" : String.format(Locale.ROOT, "%.0f", (double) outageTicks / gaps), longest);
    }

    private static String bugTable(int seeds, long ticks) {
        Map<String, RaftConfig> variants = new LinkedHashMap<>();
        variants.put("Correct Raft", RaftConfig.correct());
        variants.put("Correct Raft, no no-op entry (control)", RaftConfig.correct().withoutNoOp());
        variants.put("Skip the log up-to-date check when voting", RaftConfig.correct().withoutLogUpToDateCheck());
        variants.put("Forget the vote after a restart", RaftConfig.correct().withoutVotePersistence());
        variants.put("Commit old-term entries by counting replicas", RaftConfig.correct().withCommitOfOldTerms());
        variants.put("Commit old-term entries, and no no-op entry", RaftConfig.correct().withCommitOfOldTerms().withoutNoOp());

        StringBuilder sb = new StringBuilder("| Variant | Runs with a violation | First violation found |\n|---|---:|---|\n");
        for (Map.Entry<String, RaftConfig> variant : variants.entrySet()) {
            int caught = 0;
            String firstFound = "none";
            for (int seed = 0; seed < seeds; seed++) {
                RunResult r = new Cluster(hard(ticks), variant.getValue(), seed).run();
                if (!r.safe()) {
                    if (caught == 0) {
                        firstFound = "seed " + seed + ": " + r.violation();
                    }
                    caught++;
                }
            }
            sb.append(String.format(Locale.ROOT, "| %s | %d / %d | %s |%n", variant.getKey(), caught, seeds, firstFound));
        }
        return sb.toString();
    }

    private static int intArg(String[] args, String name, int fallback) {
        return Integer.parseInt(stringArg(args, name, String.valueOf(fallback)));
    }

    private static String stringArg(String[] args, String name, String fallback) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
