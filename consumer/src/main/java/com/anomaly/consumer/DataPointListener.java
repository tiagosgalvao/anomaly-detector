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

    // idIsGroup = false: the id names the container only. Left at its default, spring-kafka would use it
    // as the consumer group id too, silently overriding spring.kafka.consumer.group-id.
    @KafkaListener(id = CONTAINER_ID, idIsGroup = false, topics = "${consumer.topic}")
    void onDataPoint(DataPoint dataPoint, Acknowledgment acknowledgment) {
        log.info("received data point id:{} series:{} value:{} timestamp:{}",
                dataPoint.id(),
                dataPoint.seriesId(),
                dataPoint.value(),
                dataPoint.timestamp());
        acknowledgment.acknowledge();
    }
}
