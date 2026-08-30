package com.anomaly.consumer;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaErrorHandlingConfiguration {

    static final String DEAD_LETTER_SUFFIX = ".DLT";

    private static final int ANY_PARTITION = -1;

    private static final int PARTITIONS = 1;
    private static final int REPLICAS = 1;

    @Bean
    NewTopic deadLetterTopic(ConsumerProperties properties) {
        return TopicBuilder.name(properties.topic() + DEAD_LETTER_SUFFIX)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    KafkaTemplate<String, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        var valueSerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                DataPoint.class, new JacksonJsonSerializer<DataPoint>()));
        var producerFactory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(), new StringSerializer(), valueSerializer);
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    DefaultErrorHandler deadLetterErrorHandler(KafkaTemplate<String, Object> deadLetterKafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate,
                (rec, _) -> new TopicPartition(rec.topic() + DEAD_LETTER_SUFFIX, ANY_PARTITION));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L));
    }
}
