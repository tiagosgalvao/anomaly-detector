package com.anomaly.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class DataPointDeserializationTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void readsTheFieldsItModels() {
        var json = """
                {"id":"a-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":50.25}
                """;

        var dataPoint = jsonMapper.readValue(json, DataPoint.class);

        assertThat(dataPoint.id()).isEqualTo("a-1");
        assertThat(dataPoint.seriesId()).isEqualTo("sensor-1");
        assertThat(dataPoint.value()).isEqualTo(50.25);
        assertThat(dataPoint.timestamp()).isEqualTo("2026-01-01T00:00:00Z");
    }

    @Test
    void ignoresFieldsTheProducerAddsThatItDoesNotModel() {
        var json = """
                {"id":"a-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":50.25,
                 "injectedAnomaly":true,"somethingAddedLater":"whatever"}
                """;

        var dataPoint = jsonMapper.readValue(json, DataPoint.class);

        assertThat(dataPoint.value()).isEqualTo(50.25);
    }

    @Test
    void leavesAnAbsentValueNullRatherThanFabricatingZero() {
        var json = """
                {"id":"a-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z"}
                """;

        var dataPoint = jsonMapper.readValue(json, DataPoint.class);

        assertThat(dataPoint.value()).isNull();
    }

    @Test
    void leavesAnExplicitlyNullValueNull() {
        var json = jsonMapper.writeValueAsString(new DataPoint("a-1", "sensor-1", Instant.parse("2026-01-01T00:00:00Z"), null));

        assertThat(json).contains("\"value\":null");

        var dataPoint = jsonMapper.readValue(json, DataPoint.class);

        assertThat(dataPoint.value()).isNull();
    }

    @Test
    void surfacesAnOverflowingNumberAsInfinityRatherThanFailing() {
        var json = """
                {"id":"a-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":1e400}
                """;

        var dataPoint = jsonMapper.readValue(json, DataPoint.class);

        assertThat(dataPoint.value()).isInfinite();
    }

    @Test
    void rejectsANonNumericValue() {
        var json = """
                {"id":"a-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":"warm"}
                """;

        assertThatThrownBy(() -> jsonMapper.readValue(json, DataPoint.class)).isInstanceOf(Exception.class);
    }
}
