package com.anomaly.producer;

import java.time.Instant;

public record DataPoint(
        String id,
        String seriesId,
        Instant timestamp,
        double value,
        boolean injectedAnomaly) {
}
