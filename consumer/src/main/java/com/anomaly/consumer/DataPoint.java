package com.anomaly.consumer;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DataPoint(
        String id,
        String seriesId,
        Instant timestamp,
        Double value) {
}
