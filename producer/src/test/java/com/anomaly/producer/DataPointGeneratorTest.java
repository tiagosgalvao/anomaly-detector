package com.anomaly.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class DataPointGeneratorTest {

    private static final long SEED = 42L;
    private static final double MEAN = 50.0;
    private static final double STANDARD_DEVIATION = 5.0;
    private static final double ANOMALY_PROBABILITY = 0.02;
    private static final double MIN_SIGMA_MULTIPLIER = 6.0;
    private static final double MAX_SIGMA_MULTIPLIER = 12.0;
    private static final int SAMPLE_SIZE = 100_000;

    @Test
    void generatesTheSameSequenceForTheSameSeed() {
        var first = generate(seeded(SEED), 100).stream().map(DataPoint::value).toList();
        var second = generate(seeded(SEED), 100).stream().map(DataPoint::value).toList();

        assertThat(first).isEqualTo(second);
    }

    @Test
    void generatesDifferentSequencesForDifferentSeeds() {
        var first = generate(seeded(SEED), 100).stream().map(DataPoint::value).toList();
        var second = generate(seeded(SEED + 1), 100).stream().map(DataPoint::value).toList();

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void neverGeneratesNonFiniteValues() {
        assertThat(generate(seeded(SEED), SAMPLE_SIZE))
                .allSatisfy(point -> assertThat(Double.isFinite(point.value())).isTrue());
    }

    @Test
    void baselineValuesFollowTheConfiguredNormalDistribution() {
        var baseline = generate(seeded(SEED), SAMPLE_SIZE).stream()
                .filter(point -> !point.injectedAnomaly())
                .map(DataPoint::value)
                .toList();

        assertThat(mean(baseline)).isCloseTo(MEAN, within(0.1));
        assertThat(standardDeviation(baseline)).isCloseTo(STANDARD_DEVIATION, within(0.1));
    }

    @Test
    void injectsAnomaliesAtRoughlyTheConfiguredRate() {
        var points = generate(seeded(SEED), SAMPLE_SIZE);

        var injected = points.stream().filter(DataPoint::injectedAnomaly).count();

        assertThat((double) injected / SAMPLE_SIZE).isCloseTo(ANOMALY_PROBABILITY, within(0.002));
    }

    @Test
    void injectedAnomaliesLieFarEnoughOutToBeDetectable() {
        var anomalies = generate(seeded(SEED), SAMPLE_SIZE).stream()
                .filter(DataPoint::injectedAnomaly)
                .toList();

        assertThat(anomalies).isNotEmpty();
        assertThat(anomalies).allSatisfy(point -> {
            var deviationInSigmas = Math.abs(point.value() - MEAN) / STANDARD_DEVIATION;
            assertThat(deviationInSigmas).isBetween(MIN_SIGMA_MULTIPLIER, MAX_SIGMA_MULTIPLIER);
        });
    }

    @Test
    void injectsAnomaliesOnBothSidesOfTheMean() {
        var anomalies = generate(seeded(SEED), SAMPLE_SIZE).stream()
                .filter(DataPoint::injectedAnomaly)
                .map(DataPoint::value)
                .toList();

        assertThat(anomalies).anyMatch(value -> value > MEAN);
        assertThat(anomalies).anyMatch(value -> value < MEAN);
    }

    @Test
    void stampsEveryPointWithTheConfiguredSeriesAndAUniqueId() {
        var points = generate(seeded(SEED), 1_000);

        assertThat(points).allSatisfy(point -> assertThat(point.seriesId()).isEqualTo("sensor-1"));
        assertThat(points).extracting(DataPoint::id).doesNotHaveDuplicates();
    }

    private static double mean(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }

    private static double standardDeviation(List<Double> values) {
        var mean = mean(values);
        var sumOfSquaredDeviations = values.stream()
                .mapToDouble(value -> (value - mean) * (value - mean))
                .sum();
        return Math.sqrt(sumOfSquaredDeviations / (values.size() - 1));
    }

    private static List<DataPoint> generate(DataPointGenerator generator, int count) {
        return IntStream.range(0, count).mapToObj(i -> generator.next()).toList();
    }

    private static DataPointGenerator seeded(long seed) {
        return new DataPointGenerator(new ProducerProperties(
                "metrics.raw",
                "sensor-1",
                100L,
                MEAN,
                STANDARD_DEVIATION,
                seed,
                new ProducerProperties.Anomaly(ANOMALY_PROBABILITY, MIN_SIGMA_MULTIPLIER, MAX_SIGMA_MULTIPLIER)));
    }
}
