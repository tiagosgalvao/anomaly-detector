package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.ANOMALY;
import static com.anomaly.consumer.Detection.Status.NORMAL;
import static com.anomaly.consumer.Detection.Status.WARMING_UP;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class SeriesDetectorsTest {

    private final SeriesDetectors detectors = new SeriesDetectors(new ConsumerProperties("metrics.raw", 10, 3.0, 10));

    @Test
    void keepsASeparateBaselinePerSeries() {
        warmUp("sensor-1", 50.0);
        warmUp("sensor-2", 1000.0);

        assertThat(detectors.evaluate("sensor-1", 50.0).status()).isEqualTo(NORMAL);
        assertThat(detectors.evaluate("sensor-2", 1000.0).status()).isEqualTo(NORMAL);
    }

    @Test
    void doesNotLetOneSeriesMakeAnotherLookAnomalous() {
        warmUp("sensor-1", 50.0);

        // Normal for sensor-2, which has its own window, but wildly out of range for sensor-1's.
        var freshSeries = detectors.evaluate("sensor-2", 1000.0);

        assertThat(freshSeries.status()).isEqualTo(WARMING_UP);
    }

    @Test
    void detectsAnAnomalyAgainstTheRightSeriesBaseline() {
        warmUp("sensor-1", 50.0);
        detectors.evaluate("sensor-1", 51.0);

        var detection = detectors.evaluate("sensor-1", 5000.0);

        assertThat(detection.status()).isEqualTo(ANOMALY);
    }

    private void warmUp(String seriesId, double around) {
        IntStream.range(0, 10)
                .forEach(i -> detectors.evaluate(seriesId, around + (i % 2 == 0 ? 0.5 : -0.5)));
    }
}
