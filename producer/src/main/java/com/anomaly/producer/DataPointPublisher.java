package com.anomaly.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class DataPointPublisher {

    private static final Logger log = LoggerFactory.getLogger(DataPointPublisher.class);

    private final KafkaTemplate<String, DataPoint> kafkaTemplate;
    private final DataPointGenerator generator;
    private final ProducerProperties properties;

    DataPointPublisher(KafkaTemplate<String, DataPoint> kafkaTemplate,
                       DataPointGenerator generator,
                       ProducerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.generator = generator;
        this.properties = properties;
    }

    @Scheduled(fixedRateString = "${producer.interval-ms}")
    void publishNextDataPoint() {
        var dataPoint = generator.next();
        if (!Double.isFinite(dataPoint.value())) {
            log.error("discarded non-finite data point id:{} value:{}", dataPoint.id(), dataPoint.value());
            return;
        }
        kafkaTemplate.send(properties.topic(), dataPoint.seriesId(), dataPoint)
                .whenComplete((result, failure) -> logOutcome(dataPoint, result, failure));
    }

    private void logOutcome(DataPoint dataPoint, SendResult<String, DataPoint> result, Throwable failure) {
        if (failure != null) {
            log.error("failed to publish data point id:{}", dataPoint.id(), failure);
            return;
        }
        log.debug("published data point id:{} value:{} anomaly:{} partition:{} offset:{}",
                dataPoint.id(),
                dataPoint.value(),
                dataPoint.injectedAnomaly(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
    }
}
