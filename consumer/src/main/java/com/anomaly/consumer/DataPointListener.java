package com.anomaly.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class DataPointListener {

    private static final Logger log = LoggerFactory.getLogger(DataPointListener.class);

    static final String CONTAINER_ID = "dataPoints";

    private final SeriesDetectors seriesDetectors;
    private final DetectionReporter reporter;

    DataPointListener(SeriesDetectors seriesDetectors, DetectionReporter reporter) {
        this.seriesDetectors = seriesDetectors;
        this.reporter = reporter;
    }

    // idIsGroup = false: the id names the container only. Left at its default, spring-kafka would use it
    // as the consumer group id too, silently overriding spring.kafka.consumer.group-id.
    @KafkaListener(id = CONTAINER_ID, idIsGroup = false, topics = "${consumer.topic}")
    void onDataPoint(DataPoint dataPoint, Acknowledgment acknowledgment) {
        log.debug("received data point id:{} series:{} value:{}",
                dataPoint.id(), dataPoint.seriesId(), dataPoint.value());

        var detection = seriesDetectors.evaluate(dataPoint.seriesId(), dataPoint.value());
        reporter.report(dataPoint.timestamp(), detection);

        acknowledgment.acknowledge();
    }
}
