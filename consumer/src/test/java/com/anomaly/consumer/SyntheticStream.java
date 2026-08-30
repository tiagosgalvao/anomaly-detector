package com.anomaly.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

final class SyntheticStream {

    static final double MEAN = 50.0;
    static final double STANDARD_DEVIATION = 5.0;
    static final double ANOMALY_PROBABILITY = 0.02;
    static final double MIN_SIGMA = 6.0;
    static final double MAX_SIGMA = 12.0;

    private SyntheticStream() {
    }

    record Reading(double value, boolean injectedAnomaly) {
    }

    static List<Reading> of(int size, long seed) {
        var random = new Random(seed);
        var readings = new ArrayList<Reading>(size);
        for (var i = 0; i < size; i++) {
            var injected = random.nextDouble() < ANOMALY_PROBABILITY;
            readings.add(new Reading(injected ? outlier(random) : baseline(random), injected));
        }
        return readings;
    }

    private static double baseline(Random random) {
        return MEAN + random.nextGaussian() * STANDARD_DEVIATION;
    }

    private static double outlier(Random random) {
        var multiplier = MIN_SIGMA + random.nextDouble() * (MAX_SIGMA - MIN_SIGMA);
        var sign = random.nextBoolean() ? 1 : -1;
        return MEAN + sign * multiplier * STANDARD_DEVIATION;
    }
}
