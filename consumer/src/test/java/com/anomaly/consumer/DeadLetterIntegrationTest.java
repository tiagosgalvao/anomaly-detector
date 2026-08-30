package com.anomaly.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = {"metrics.raw", "metrics.raw.DLT"}, partitions = 1)
@SuppressWarnings("java:S5663")
class DeadLetterIntegrationTest {

    private static final String TOPIC = "metrics.raw";
    private static final String DEAD_LETTER_TOPIC = "metrics.raw.DLT";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Autowired
    private EmbeddedKafkaBroker broker;

    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<String, String> deadLetterConsumer;

    @BeforeEach
    void setUp() {
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        deadLetterConsumer = new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps(broker, "dead-letter-test-" + System.nanoTime(), true),
                new StringDeserializer(), new StringDeserializer()).createConsumer();
        deadLetterConsumer.subscribe(List.of(DEAD_LETTER_TOPIC));
    }

    @AfterEach
    void tearDown() {
        deadLetterConsumer.close();
    }

    @Test
    void routesUnparseablePayloadsToTheDeadLetterTopic() {
        publish("this is not json at all");

        assertThat(deadLettered()).contains("not json");
    }

    @Test
    void routesAPayloadWhoseValueIsNotANumber() {
        publish("""
                {"id":"bad-1","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":"warm"}""");

        assertThat(deadLettered()).contains("bad-1");
    }

    @Test
    void routesAWellFormedPayloadThatIsMissingItsValue() {
        publish("""
                {"id":"bad-2","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z"}""");

        assertThat(deadLettered()).contains("bad-2");
    }

    @Test
    void keepsProcessingGoodRecordsPublishedAfterABadOne() {
        publish("still not json");
        deadLettered();

        publish(jsonMapper.writeValueAsString(
                new DataPoint("good-1", "sensor-1", Instant.parse("2026-01-01T00:00:00Z"), 50.0)));

        assertThat(pollDeadLetter(Duration.ofSeconds(3))).isEmpty();
    }

    private String deadLettered() {
        var records = await().atMost(TIMEOUT)
                .until(() -> pollDeadLetter(Duration.ofSeconds(1)), values -> !values.isEmpty());
        return String.join("\n", records);
    }

    private List<String> pollDeadLetter(Duration timeout) {
        var records = deadLetterConsumer.poll(timeout);
        return java.util.stream.StreamSupport.stream(records.spliterator(), false)
                .map(rec -> rec.value() == null ? "" : rec.value())
                .toList();
    }

    private void publish(String payload) {
        kafkaTemplate.send(TOPIC, "sensor-1", payload).join();
    }
}
