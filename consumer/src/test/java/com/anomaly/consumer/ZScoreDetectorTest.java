package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.ANOMALY;
import static com.anomaly.consumer.Detection.Status.NORMAL;
import static com.anomaly.consumer.Detection.Status.WARMING_UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ZScoreDetectorTest {

    private static final double THRESHOLD = 3.0;
    private static final int WINDOW_SIZE = 50;
    private static final double TOLERANCE = 1e-9;

    private static final double ZERO_MEAN_UNIT_DEVIATION_PAIR_VALUE = 0.7071067811865476;

    @Test
    void reportsWarmingUpUntilItHasEnoughSamples() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, 10);

        for (var i = 0; i < 9; i++) {
            assertThat(detector.evaluate(50.0).status()).isEqualTo(WARMING_UP);
            assertThat(detector.isWarmedUp()).isFalse();
        }

        assertThat(detector.evaluate(50.0).status()).isEqualTo(WARMING_UP);
        assertThat(detector.isWarmedUp()).isTrue();
    }

    @Test
    void scoresAValueAgainstTheWindowAsItStoodBeforeThatValueArrived() {
        var detector = zeroMeanUnitDeviationDetector();

        var detection = detector.evaluate(3.0);

        assertThat(detection.mean()).isCloseTo(0.0, within(TOLERANCE));
        assertThat(detection.standardDeviation()).isCloseTo(1.0, within(TOLERANCE));
        assertThat(detection.zScore()).isCloseTo(3.0, within(TOLERANCE));
    }

    @Test
    void treatsAScoreExactlyAtTheThresholdAsNormal() {
        var detection = zeroMeanUnitDeviationDetector().evaluate(3.0);

        assertThat(detection.zScore()).isCloseTo(THRESHOLD, within(TOLERANCE));
        assertThat(detection.status()).isEqualTo(NORMAL);
    }

    @Test
    void flagsAScoreJustAboveTheThreshold() {
        var detection = zeroMeanUnitDeviationDetector().evaluate(3.000001);

        assertThat(detection.zScore()).isGreaterThan(THRESHOLD);
        assertThat(detection.status()).isEqualTo(ANOMALY);
        assertThat(detection.isAnomaly()).isTrue();
    }

    @Test
    void keepsAnomaliesOutOfTheWindowSoTheyCannotInflateTheDeviation() {
        var detector = detectorWarmedUpOscillatingAroundFifty();

        var anomaly = detector.evaluate(500.0);
        var next = detector.evaluate(50.0);

        assertThat(anomaly.status()).isEqualTo(ANOMALY);
        assertThat(next.mean()).isCloseTo(anomaly.mean(), within(TOLERANCE));
        assertThat(next.standardDeviation()).isCloseTo(anomaly.standardDeviation(), within(TOLERANCE));
    }

    @Test
    void admitsNormalValuesSoTheBaselineTracksTheSeries() {
        var detector = detectorWarmedUpOscillatingAroundFifty();

        var first = detector.evaluate(50.5);
        var second = detector.evaluate(50.5);

        assertThat(first.status()).isEqualTo(NORMAL);
        assertThat(second.mean()).isNotCloseTo(first.mean(), within(TOLERANCE));
    }

    @Test
    void detectsDropsAsReadilyAsSpikes() {
        assertThat(zeroMeanUnitDeviationDetector().evaluate(-4.0).status()).isEqualTo(ANOMALY);
        assertThat(zeroMeanUnitDeviationDetector().evaluate(4.0).status()).isEqualTo(ANOMALY);
    }

    @Test
    void neverDividesByZeroWhenEveryValueInTheWindowIsIdentical() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, 10);
        IntStream.range(0, 10).forEach(i -> detector.evaluate(42.0));

        var detection = detector.evaluate(9999.0);

        assertThat(detection.zScore()).isZero();
        assertThat(detection.status()).isEqualTo(NORMAL);
    }

    @Test
    void neverProducesANonFiniteScore() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, 10);

        var scores = IntStream.range(0, 5_000)
                .mapToObj(i -> detector.evaluate(i % 7 == 0 ? 1_000_000.0 : 50.0 + (i % 5)))
                .map(Detection::zScore)
                .toList();

        assertThat(scores).isNotEmpty().allSatisfy(score -> assertThat(Double.isFinite(score)).isTrue());
    }

    @Test
    void isNotCappedByTheWindowItScoresAgainst() {
        var detector = detectorWarmedUpOscillatingAroundFifty();

        var detection = detector.evaluate(50_000.0);

        assertThat(detection.zScore()).isGreaterThan(100.0);
    }

    @Test
    void rejectsConfigurationItCannotDetectWith() {
        assertThatThrownBy(() -> new ZScoreDetector(WINDOW_SIZE, 0.0, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ZScoreDetector(WINDOW_SIZE, THRESHOLD, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ZScoreDetector(10, THRESHOLD, 11))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ZScoreDetector zeroMeanUnitDeviationDetector() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, 2);
        detector.evaluate(-ZERO_MEAN_UNIT_DEVIATION_PAIR_VALUE);
        detector.evaluate(ZERO_MEAN_UNIT_DEVIATION_PAIR_VALUE);
        return detector;
    }

    private static ZScoreDetector detectorWarmedUpOscillatingAroundFifty() {
        var detector = new ZScoreDetector(WINDOW_SIZE, THRESHOLD, WINDOW_SIZE);
        IntStream.range(0, WINDOW_SIZE).forEach(i -> detector.evaluate(50.0 + (i % 2 == 0 ? 1 : -1)));
        return detector;
    }
}
