package com.anomaly.producer;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("producer")
record ProducerProperties(
        String topic,
        String seriesId,
        long intervalMs,
        double mean,
        double standardDeviation,
        Long seed,
        Anomaly anomaly) {

    ProducerProperties {
        if (!Double.isFinite(mean)) {
            throw new IllegalArgumentException("producer.mean must be finite, was " + mean);
        }
        if (standardDeviation > 0) {
            if (intervalMs <= 0) {
                throw new IllegalArgumentException("producer.interval-ms must be positive, was " + intervalMs);
            }
        } else {
            throw new IllegalArgumentException("producer.standard-deviation must be positive, was " + standardDeviation);
        }
    }

    record Anomaly(double probability, double minSigmaMultiplier, double maxSigmaMultiplier) {

        Anomaly {
            if (probability < 0 || probability > 1) {
                throw new IllegalArgumentException("producer.anomaly.probability must be between 0 and 1, was " + probability);
            }
            if (minSigmaMultiplier <= 0) {
                throw new IllegalArgumentException("producer.anomaly.min-sigma-multiplier must be positive, was " + minSigmaMultiplier);
            }
            if (maxSigmaMultiplier < minSigmaMultiplier) {
                throw new IllegalArgumentException("producer.anomaly.max-sigma-multiplier must not be below the minimum, was " + maxSigmaMultiplier);
            }
        }
    }
}
