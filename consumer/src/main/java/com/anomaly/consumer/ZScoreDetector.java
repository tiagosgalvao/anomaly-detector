package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.ANOMALY;
import static com.anomaly.consumer.Detection.Status.NORMAL;
import static com.anomaly.consumer.Detection.Status.WARMING_UP;

final class ZScoreDetector {

    private static final double MINIMUM_USABLE_DEVIATION = 1e-9;

    private static final double NO_DEVIATION_MEASURABLE = 0.0;

    private final RollingWindow window;
    private final double threshold;
    private final int minimumSamples;

    ZScoreDetector(int windowSize, double threshold, int minimumSamples) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("threshold must be positive, was " + threshold);
        }
        if (minimumSamples < 2 || minimumSamples > windowSize) {
            throw new IllegalArgumentException("minimum samples must be between 2 and the window size, was " + minimumSamples);
        }
        this.window = new RollingWindow(windowSize);
        this.threshold = threshold;
        this.minimumSamples = minimumSamples;
    }

    Detection evaluate(double value) {
        if (window.size() < minimumSamples) {
            window.add(value);
            return new Detection(value, Double.NaN, Double.NaN, NO_DEVIATION_MEASURABLE, WARMING_UP);
        }

        var mean = window.mean();
        var standardDeviation = window.standardDeviation();

        if (standardDeviation < MINIMUM_USABLE_DEVIATION) {
            window.add(value);
            return new Detection(value, mean, standardDeviation, NO_DEVIATION_MEASURABLE, NORMAL);
        }

        var zScore = Math.abs(value - mean) / standardDeviation;
        if (zScore > threshold) {
            return new Detection(value, mean, standardDeviation, zScore, ANOMALY);
        }

        window.add(value);
        return new Detection(value, mean, standardDeviation, zScore, NORMAL);
    }

    boolean isWarmedUp() {
        return window.size() >= minimumSamples;
    }
}
