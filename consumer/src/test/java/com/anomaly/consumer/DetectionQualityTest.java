package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.WARMING_UP;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DetectionQualityTest {

    private static final long SEED = 20260829L;
    private static final int SAMPLE_SIZE = 200_000;

    private static final int WINDOW_SIZE = 50;
    private static final double THRESHOLD = 3.0;

    @Test
    void catchesEssentiallyEveryInjectedAnomaly() {
        var quality = measure();

        assertThat(quality.recall()).isGreaterThan(0.99);
    }

    @Test
    void raisesFalseAlarmsWellAboveTheTextbookRateForReasonsWorthKnowing() {
        var quality = measure();

        assertThat(quality.falsePositiveRate()).isBetween(0.004, 0.009);
        assertThat(quality.falsePositiveRate()).isGreaterThan(0.0027);
    }

    @Test
    void keepsPrecisionHighEnoughForTheAlertsToBeWorthReading() {
        var quality = measure();

        assertThat(quality.precision()).isGreaterThan(0.75);
    }

    @Test
    void neverScoresAPointAsNonFinite() {
        assertThat(measure().nonFiniteScores()).isZero();
    }

    private static Quality measure() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, WINDOW_SIZE);

        var truePositives = 0;
        var falsePositives = 0;
        var falseNegatives = 0;
        var baselinePoints = 0;
        var nonFiniteScores = 0;

        for (var reading : SyntheticStream.of(SAMPLE_SIZE, SEED)) {
            var injected = reading.injectedAnomaly();
            var detection = detector.evaluate(reading.value());
            if (detection.status() == WARMING_UP) {
                continue;
            }
            if (!Double.isFinite(detection.zScore())) {
                nonFiniteScores++;
            }

            if (injected) {
                if (detection.isAnomaly()) {
                    truePositives++;
                } else {
                    falseNegatives++;
                }
            } else {
                baselinePoints++;
                if (detection.isAnomaly()) {
                    falsePositives++;
                }
            }
        }
        return new Quality(truePositives, falsePositives, falseNegatives, baselinePoints, nonFiniteScores);
    }

    private record Quality(int truePositives, int falsePositives, int falseNegatives, int baselinePoints,
                           int nonFiniteScores) {

        double recall() {
            return (double) truePositives / (truePositives + falseNegatives);
        }

        double precision() {
            return (double) truePositives / (truePositives + falsePositives);
        }

        double falsePositiveRate() {
            return (double) falsePositives / baselinePoints;
        }
    }
}
