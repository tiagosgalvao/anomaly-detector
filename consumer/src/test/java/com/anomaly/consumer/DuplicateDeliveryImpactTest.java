package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.ANOMALY;
import static com.anomaly.consumer.Detection.Status.WARMING_UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class DuplicateDeliveryImpactTest {

    private static final long SEED = 20260830L;
    private static final int SAMPLE_SIZE = 60_000;
    private static final int WINDOW_SIZE = 50;
    private static final double THRESHOLD = 3.0;

    private static final double DUPLICATE_RATE = 0.01;
    private static final double MAX_ACCEPTABLE_VERDICT_CHANGE_RATE = 0.001;

    @Test
    void redeliveryChangesAlmostNoDetectionVerdicts() {
        var readings = SyntheticStream.of(SAMPLE_SIZE, SEED);

        var clean = verdicts(readings);
        var withDuplicates = verdicts(duplicate(readings));

        var changed = countChangedVerdicts(clean, withDuplicates, readings);

        assertThat(changed.rate()).isLessThan(MAX_ACCEPTABLE_VERDICT_CHANGE_RATE);
    }

    @Test
    void aRedeliveredAnomalyCannotDisturbTheWindowAtAll() {
        var detector = warmedUpDetector();

        var first = detector.evaluate(500.0);
        var redelivered = detector.evaluate(500.0);
        var next = detector.evaluate(50.0);

        assertThat(first.status()).isEqualTo(ANOMALY);
        assertThat(redelivered.mean()).isCloseTo(first.mean(), within(1e-9));
        assertThat(redelivered.standardDeviation()).isCloseTo(first.standardDeviation(), within(1e-9));
        assertThat(next.mean()).isCloseTo(first.mean(), within(1e-9));
    }

    @Test
    void aRedeliveredNormalReadingMovesTheBaselineOnlyMarginally() {
        var once = warmedUpDetector();
        var twice = warmedUpDetector();

        once.evaluate(65.0);
        twice.evaluate(65.0);
        twice.evaluate(65.0);

        var withoutDuplicate = once.evaluate(50.0).mean();
        var withDuplicate = twice.evaluate(50.0).mean();

        var shiftInSigmas = Math.abs(withDuplicate - withoutDuplicate) / SyntheticStream.STANDARD_DEVIATION;
        assertThat(shiftInSigmas).isLessThan(0.2);
    }

    private record Changed(int count, int compared) {
        double rate() {
            return (double) count / compared;
        }
    }

    private static Changed countChangedVerdicts(List<Boolean> clean, List<Boolean> withDuplicates,
                                                List<SyntheticStream.Reading> readings) {
        var random = new Random(SEED + 1);
        var changed = 0;
        var compared = 0;
        var duplicatedIndex = 0;
        for (var i = 0; i < readings.size(); i++) {
            var cleanVerdict = clean.get(i);
            var duplicatedVerdict = withDuplicates.get(duplicatedIndex);
            if (cleanVerdict != null && duplicatedVerdict != null) {
                compared++;
                if (!cleanVerdict.equals(duplicatedVerdict)) {
                    changed++;
                }
            }
            duplicatedIndex++;
            if (random.nextDouble() < DUPLICATE_RATE) {
                duplicatedIndex++;
            }
        }
        return new Changed(changed, compared);
    }

    private static List<SyntheticStream.Reading> duplicate(List<SyntheticStream.Reading> readings) {
        var random = new Random(SEED + 1);
        var withDuplicates = new ArrayList<SyntheticStream.Reading>(readings.size() * 2);
        for (var reading : readings) {
            withDuplicates.add(reading);
            if (random.nextDouble() < DUPLICATE_RATE) {
                withDuplicates.add(reading);
            }
        }
        return withDuplicates;
    }

    private static List<Boolean> verdicts(List<SyntheticStream.Reading> readings) {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, WINDOW_SIZE);
        var results = new ArrayList<Boolean>(readings.size());
        for (var reading : readings) {
            var detection = detector.evaluate(reading.value());
            results.add(detection.status() == WARMING_UP ? null : detection.isAnomaly());
        }
        return results;
    }

    private static ZScoreDetector warmedUpDetector() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, WINDOW_SIZE);
        for (var reading : SyntheticStream.of(WINDOW_SIZE, SEED)) {
            detector.evaluate(reading.value());
        }
        return detector;
    }
}
