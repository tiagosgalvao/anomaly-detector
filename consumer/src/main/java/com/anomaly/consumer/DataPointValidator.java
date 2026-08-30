package com.anomaly.consumer;

import org.springframework.stereotype.Component;

@Component
class DataPointValidator {

    void validate(DataPoint dataPoint) {
        if (dataPoint.seriesId() == null || dataPoint.seriesId().isBlank()) {
            throw new InvalidDataPointException("data point has no series id: " + dataPoint.id());
        }
        if (dataPoint.timestamp() == null) {
            throw new InvalidDataPointException("data point has no timestamp: " + dataPoint.id());
        }
        if (dataPoint.value() == null) {
            throw new InvalidDataPointException("data point has no value: " + dataPoint.id());
        }
        if (!Double.isFinite(dataPoint.value())) {
            throw new InvalidDataPointException("data point value is not finite: " + dataPoint.id() + " = " + dataPoint.value());
        }
    }
}
