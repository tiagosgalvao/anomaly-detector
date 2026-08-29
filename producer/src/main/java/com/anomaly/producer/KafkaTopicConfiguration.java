package com.anomaly.producer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfiguration {

    // One logical series, and a rolling window is order-sensitive:
    // extra partitions would buy parallelism the consumer cannot use while breaking the ordering it depends on.
    private static final int PARTITIONS = 1;
    private static final int REPLICAS = 1;

    @Bean
    NewTopic metricsTopic(ProducerProperties properties) {
        return TopicBuilder.name(properties.topic())
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
}
