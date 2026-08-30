package com.anomaly.consumer;

import static com.anomaly.consumer.DataPointListener.CONTAINER_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

/**
 * The two delivery guarantees the broker choice rests on: that consumed data can be re-read, and that
 * nothing is lost while the consumer is away. See decisions 2 and 13.
 */
@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = "metrics.raw", partitions = 1)
class ReplayIntegrationTest {

    private static final String TOPIC = "metrics.raw";
    private static final String GROUP = "anomaly-detector";
    private static final String SERIES = "sensor-1";
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(20);

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @MockitoSpyBean
    private DataPointListener listener;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void createProducer() {
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
    }

    @Test
    void replaysTheSameRecordsAfterTheConsumerGroupOffsetIsReset() {
        var published = publishSeries("first-pass", 5);
        awaitReceipt(published);

        rewindGroupToBeginning();

        // Every one of them a second time, still in order. Counted rather than totalled: rewinding to the
        // start of the log also redelivers whatever a sibling test left on the topic.
        await().atMost(RECEIVE_TIMEOUT).untilAsserted(() -> {
            var ids = receivedIds();
            assertThat(published).allSatisfy(id -> assertThat(frequency(ids, id)).isEqualTo(2));
            assertThat(ids.subList(ids.size() - published.size(), ids.size()))
                    .containsExactlyElementsOf(published);
        });
    }

    @Test
    void losesNothingPublishedWhileTheConsumerIsStopped() {
        stopListening();

        var published = publishSeries("while-stopped", 3);
        startListening();

        awaitReceipt(published);
        assertThat(receivedIds()).containsSubsequence(published);
    }

    /**
     * The API equivalent of {@code kafka-consumer-groups.sh --reset-offsets --to-earliest --execute}. The
     * container has to be stopped first: Kafka refuses to alter offsets for a group with live members.
     */
    private void rewindGroupToBeginning() {
        stopListening();
        try (var admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            admin.alterConsumerGroupOffsets(GROUP,
                            Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(0)))
                    .all()
                    .get();
        } catch (Exception e) {
            throw new IllegalStateException("could not rewind the consumer group", e);
        }
        startListening();
    }

    private void stopListening() {
        container().stop();
        await().atMost(RECEIVE_TIMEOUT).until(() -> !container().isRunning());
    }

    private void startListening() {
        container().start();
        await().atMost(RECEIVE_TIMEOUT).until(container()::isRunning);
    }

    private MessageListenerContainer container() {
        return registry.getListenerContainer(CONTAINER_ID);
    }

    private void awaitReceipt(List<String> ids) {
        await().atMost(RECEIVE_TIMEOUT).until(() -> receivedIds().containsAll(ids));
    }

    private static int frequency(List<String> ids, String id) {
        return Collections.frequency(ids, id);
    }

    /** {@code atLeast(0)} captures every invocation so far without asserting one has happened — the
     *  callers poll this, so it must return an empty list rather than throw before anything arrives. */
    private List<String> receivedIds() {
        var received = ArgumentCaptor.forClass(DataPoint.class);
        verify(listener, atLeast(0)).onDataPoint(received.capture(), any());
        return received.getAllValues().stream().map(DataPoint::id).toList();
    }

    private List<String> publishSeries(String idPrefix, int count) {
        var ids = IntStream.range(0, count).mapToObj(i -> idPrefix + "-" + i).toList();
        ids.forEach(id -> publish(new DataPoint(id, SERIES, Instant.parse("2026-01-01T00:00:00Z"), 50.0)));
        return ids;
    }

    /**
     * One producer for the whole test, and each send is awaited: ordering is only guaranteed within a
     * single producer, and these tests assert the exact order the detector would see.
     */
    private void publish(DataPoint dataPoint) {
        kafkaTemplate.send(TOPIC, SERIES, jsonMapper.writeValueAsString(dataPoint)).join();
    }
}
