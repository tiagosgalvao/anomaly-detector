package com.anomaly.producer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, brokerProperties = "auto.create.topics.enable=false")
class ProducerApplicationIntegrationTest {

    private static final String TOPIC = "metrics.raw";
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(20);

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void publishesDataPointsKeyedBySeriesToTheConfiguredTopic() {
        try (var consumer = subscribedConsumer()) {
            var records = KafkaTestUtils.getRecords(consumer, RECEIVE_TIMEOUT, 5);

            assertThat(records.count()).isGreaterThanOrEqualTo(5);
            assertThat(records).allSatisfy(rec -> assertThat(rec.key()).isEqualTo("sensor-1"));
            assertThat(records).allSatisfy(rec -> assertThat(rec.partition()).isZero());
        }
    }

    @Test
    void publishesTheAgreedJsonPayload() {
        try (var consumer = subscribedConsumer()) {
            var payload = jsonMapper.readTree(firstRecord(consumer).value());

            assertThat(payload.propertyNames())
                    .containsExactlyInAnyOrder("id", "seriesId", "timestamp", "value", "injectedAnomaly");
            assertThat(payload.get("seriesId").asString()).isEqualTo("sensor-1");
            assertThat(payload.get("value").isNumber()).isTrue();
            assertThat(payload.get("value").asDouble()).isFinite();
            assertThat(payload.get("injectedAnomaly").isBoolean()).isTrue();
        }
    }

    @Test
    void writesTheTimestampAsAnIsoInstantRatherThanAnEpochNumber() {
        try (var consumer = subscribedConsumer()) {
            var timestamp = jsonMapper.readTree(firstRecord(consumer).value()).get("timestamp").asString();

            assertThat(Instant.parse(timestamp)).isBefore(Instant.now().plusSeconds(1));
        }
    }

    @Test
    void doesNotLeakAProducerSideTypeHeaderOntoTheWire() {
        try (var consumer = subscribedConsumer()) {
            assertThat(firstRecord(consumer).headers().lastHeader("__TypeId__")).isNull();
        }
    }

    private ConsumerRecord<String, String> firstRecord(Consumer<String, String> consumer) {
        return KafkaTestUtils.getRecords(consumer, RECEIVE_TIMEOUT, 1).iterator().next();
    }

    private Consumer<String, String> subscribedConsumer() {
        var properties = KafkaTestUtils.consumerProps(broker, "integration-test", true);
        var consumer = new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
        consumer.subscribe(List.of(TOPIC));
        return consumer;
    }
}
