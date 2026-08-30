package com.anomaly.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
class SeriesDetectors {

    private final Map<String, ZScoreDetector> detectorsBySeries = new ConcurrentHashMap<>();
    private final ConsumerProperties properties;

    SeriesDetectors(ConsumerProperties properties) {
        this.properties = properties;
    }

    Detection evaluate(String seriesId, double value) {
        return detectorsBySeries.computeIfAbsent(seriesId, _ -> newDetector()).evaluate(value);
    }

    private ZScoreDetector newDetector() {
        return new ZScoreDetector(properties.windowSize(), properties.threshold(), properties.minimumSamples());
    }
}
