package com.anomaly.consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class DataPointValidatorTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");

    private final DataPointValidator validator = new DataPointValidator();

    @Test
    void acceptsAWellFormedDataPoint() {
        assertThatCode(() -> validator.validate(new DataPoint("a", "sensor-1", TIMESTAMP, 50.0)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsAMissingValueRatherThanTreatingItAsZero() {
        assertThatThrownBy(() -> validator.validate(new DataPoint("a", "sensor-1", TIMESTAMP, null)))
                .isInstanceOf(InvalidDataPointException.class)
                .hasMessageContaining("no value");
    }

    @Test
    void rejectsValuesThatWouldPoisonEveryLaterStatistic() {
        assertThatThrownBy(() -> validator.validate(new DataPoint("a", "s", TIMESTAMP, Double.NaN)))
                .isInstanceOf(InvalidDataPointException.class);
        assertThatThrownBy(() -> validator.validate(
                new DataPoint("a", "s", TIMESTAMP, Double.POSITIVE_INFINITY)))
                .isInstanceOf(InvalidDataPointException.class);
        assertThatThrownBy(() -> validator.validate(
                new DataPoint("a", "s", TIMESTAMP, Double.NEGATIVE_INFINITY)))
                .isInstanceOf(InvalidDataPointException.class);
    }

    @Test
    void rejectsAPointWithNoSeriesToScoreItAgainst() {
        assertThatThrownBy(() -> validator.validate(new DataPoint("a", null, TIMESTAMP, 50.0)))
                .isInstanceOf(InvalidDataPointException.class);
        assertThatThrownBy(() -> validator.validate(new DataPoint("a", "  ", TIMESTAMP, 50.0)))
                .isInstanceOf(InvalidDataPointException.class);
    }

    @Test
    void rejectsAPointWithNoTimestampToReport() {
        assertThatThrownBy(() -> validator.validate(new DataPoint("a", "sensor-1", null, 50.0)))
                .isInstanceOf(InvalidDataPointException.class);
    }
}
