package com.anomaly.producer;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
class DataPointGenerator {

    private final ProducerProperties properties;
    private final Random random;

    DataPointGenerator(ProducerProperties properties) {
        this.properties = properties;
        this.random = properties.seed() == null ? new Random() : new Random(properties.seed());
    }

    DataPoint next() {
        var injectAnomaly = random.nextDouble() < properties.anomaly().probability();
        var value = injectAnomaly ? outlier() : baseline();
        return new DataPoint(UUID.randomUUID().toString(), properties.seriesId(), Instant.now(), value, injectAnomaly);
    }

    private double baseline() {
        return properties.mean() + random.nextGaussian() * properties.standardDeviation();
    }

    private double outlier() {
        return properties.mean() + randomSign() * sigmaMultiplier() * properties.standardDeviation();
    }

    private double sigmaMultiplier() {
        var anomaly = properties.anomaly();
        var spread = anomaly.maxSigmaMultiplier() - anomaly.minSigmaMultiplier();
        return anomaly.minSigmaMultiplier() + random.nextDouble() * spread;
    }

    private int randomSign() {
        return random.nextBoolean() ? 1 : -1;
    }
}
