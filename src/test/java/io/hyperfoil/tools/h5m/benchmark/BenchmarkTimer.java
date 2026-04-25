package io.hyperfoil.tools.h5m.benchmark;

import java.util.Arrays;

public class BenchmarkTimer {

    public record Result(String name, int iterations, double avgMs,
                         double minMs, double maxMs, double stddevMs) {}

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Exception;
    }

    /**
     * Runs warmup iterations (untimed), then measurement iterations (timed).
     * The setup runnable is called before each iteration but is NOT included in timing.
     */
    public static Result run(String name, int warmup, int measure,
                             CheckedRunnable setup, CheckedRunnable operation) throws Exception {
        // Warmup
        for (int i = 0; i < warmup; i++) {
            setup.run();
            operation.run();
        }

        // Measure
        double[] times = new double[measure];
        for (int i = 0; i < measure; i++) {
            setup.run();
            long start = System.nanoTime();
            operation.run();
            long elapsed = System.nanoTime() - start;
            times[i] = elapsed / 1_000_000.0; // convert to ms
        }

        double avg = Arrays.stream(times).average().orElse(0);
        double min = Arrays.stream(times).min().orElse(0);
        double max = Arrays.stream(times).max().orElse(0);
        double variance = Arrays.stream(times).map(t -> (t - avg) * (t - avg)).average().orElse(0);
        double stddev = Math.sqrt(variance);

        return new Result(name, measure, avg, min, max, stddev);
    }

    public static String csvHeader() {
        return "name,iterations,avg_ms,min_ms,max_ms,stddev_ms";
    }

    public static String toCsv(Result r) {
        return String.format("%s,%d,%.2f,%.2f,%.2f,%.2f",
                r.name, r.iterations, r.avgMs, r.minMs, r.maxMs, r.stddevMs);
    }
}
