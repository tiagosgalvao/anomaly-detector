package com.anomaly.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("consumer")
record ConsumerProperties(String topic, int windowSize, double threshold, int minimumSamples) {

    ConsumerProperties {
        if (windowSize < 2) {
            throw new IllegalArgumentException("consumer.window-size must be at least 2, was " + windowSize);
        }
        if (threshold <= 0) {
            throw new IllegalArgumentException("consumer.threshold must be positive, was " + threshold);
        }
        if (minimumSamples < 2 || minimumSamples > windowSize) {
            throw new IllegalArgumentException("consumer.minimum-samples must be between 2 and the window size, was " + minimumSamples);
        }
    }
}
