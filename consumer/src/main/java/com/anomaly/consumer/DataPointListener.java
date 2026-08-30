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
    private final DataPointValidator validator;

    DataPointListener(SeriesDetectors seriesDetectors, DetectionReporter reporter,
                      DataPointValidator validator) {
        this.seriesDetectors = seriesDetectors;
        this.reporter = reporter;
        this.validator = validator;
    }

    @KafkaListener(id = CONTAINER_ID, idIsGroup = false, topics = "${consumer.topic}")
    void onDataPoint(DataPoint dataPoint, Acknowledgment acknowledgment) {
        log.debug("received data point id:{} series:{} value:{} timestamp:{}",
                dataPoint.id(),
                dataPoint.seriesId(),
                dataPoint.value(),
                dataPoint.timestamp());

        validator.validate(dataPoint);

        var detection = seriesDetectors.evaluate(dataPoint.seriesId(), dataPoint.value());
        reporter.report(dataPoint.timestamp(), detection);

        acknowledgment.acknowledge();
    }
}
