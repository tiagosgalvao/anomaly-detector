package com.anomaly.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class RollingWindowTest {

    private static final double TOLERANCE = 1e-9;

    @Test
    void reportsHowManyValuesItHoldsUntilItFills() {
        var window = new RollingWindow(3);
        assertThat(window.size()).isZero();
        assertThat(window.isFull()).isFalse();

        window.add(1.0);
        window.add(2.0);
        assertThat(window.size()).isEqualTo(2);
        assertThat(window.isFull()).isFalse();

        window.add(3.0);
        assertThat(window.size()).isEqualTo(3);
        assertThat(window.isFull()).isTrue();
    }

    @Test
    void neverGrowsBeyondItsCapacity() {
        var window = new RollingWindow(50);

        IntStream.range(0, 500).forEach(window::add);

        assertThat(window.size()).isEqualTo(50);
        assertThat(window.capacity()).isEqualTo(50);
    }

    @Test
    void evictsTheOldestValueOnceFull() {
        var window = new RollingWindow(3);

        window.add(1.0);
        window.add(2.0);
        window.add(3.0);
        window.add(4.0);

        assertThat(window.mean()).isCloseTo(3.0, within(TOLERANCE));
    }

    @Test
    void computesTheMeanOfTheValuesItHolds() {
        var window = new RollingWindow(8);

        addAll(window, 2, 4, 4, 4, 5, 5, 7, 9);

        assertThat(window.mean()).isCloseTo(5.0, within(TOLERANCE));
    }

    @Test
    void computesTheSampleStandardDeviationDividingByNMinusOne() {
        var window = new RollingWindow(8);

        addAll(window, 2, 4, 4, 4, 5, 5, 7, 9);

        assertThat(window.standardDeviation()).isCloseTo(Math.sqrt(32.0 / 7.0), within(TOLERANCE));
        assertThat(window.standardDeviation()).isNotCloseTo(2.0, within(0.1));
    }

    @Test
    void reportsZeroDeviationWhenEveryValueIsIdentical() {
        var window = new RollingWindow(4);

        addAll(window, 7, 7, 7, 7);

        assertThat(window.standardDeviation()).isZero();
    }

    @Test
    void computesStatisticsOverOnlyTheSurvivingValuesAfterEviction() {
        var window = new RollingWindow(3);

        addAll(window, 100, 200, 300, 2, 4, 6);

        assertThat(window.mean()).isCloseTo(4.0, within(TOLERANCE));
        assertThat(window.standardDeviation()).isCloseTo(2.0, within(TOLERANCE));
    }

    @Test
    void rejectsACapacityTooSmallToHaveADeviation() {
        assertThatThrownBy(() -> new RollingWindow(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 2");
    }

    @Test
    void rejectsNonFiniteValuesRatherThanPoisoningEveryLaterStatistic() {
        var window = new RollingWindow(4);

        assertThatThrownBy(() -> window.add(Double.NaN)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> window.add(Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);

        addAll(window, 1, 2, 3);
        assertThat(window.mean()).isCloseTo(2.0, within(TOLERANCE));
        assertThat(window.standardDeviation()).isCloseTo(1.0, within(TOLERANCE));
    }

    @Test
    void refusesToProduceStatisticsItDoesNotHaveTheSamplesFor() {
        var window = new RollingWindow(4);

        assertThatThrownBy(window::mean).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(window::standardDeviation).isInstanceOf(IllegalStateException.class);

        window.add(5.0);
        assertThat(window.mean()).isCloseTo(5.0, within(TOLERANCE));
        assertThatThrownBy(window::standardDeviation).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void staysAccurateOverManyMoreValuesThanItHolds() {
        var window = new RollingWindow(50);

        IntStream.rangeClosed(1, 1_000_000).forEach(i -> window.add(i % 100));
        addAll(window, 10, 20, 30);

        assertThat(window.size()).isEqualTo(50);
        assertThat(Double.isFinite(window.mean())).isTrue();
        assertThat(Double.isFinite(window.standardDeviation())).isTrue();
    }

    private static void addAll(RollingWindow window, double... values) {
        for (var value : values) {
            window.add(value);
        }
    }
}
