package com.anomaly.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(topics = "metrics.raw", partitions = 1)
class ConsumerApplicationIntegrationTest {

    private static final String TOPIC = "metrics.raw";
    private static final String GROUP = "anomaly-detector";
    private static final String SERIES = "sensor-1";
    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration RECEIVE_TIMEOUT = Duration.ofSeconds(20);

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @MockitoSpyBean
    private DataPointListener listener;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void deserialisesAPublishedPayloadIntoItsOwnProjection() {
        publish(new DataPoint("a-1", SERIES, TIMESTAMP, 50.25));

        var received = receive();

        assertThat(received.id()).isEqualTo("a-1");
        assertThat(received.seriesId()).isEqualTo(SERIES);
        assertThat(received.timestamp()).isEqualTo(TIMESTAMP);
        assertThat(received.value()).isEqualTo(50.25);
    }

    @Test
    void ignoresTheProducersGroundTruthFlagRatherThanFailingOnIt() {
        publishRaw("""
                {"id":"a-2","seriesId":"sensor-1","timestamp":"2026-01-01T00:00:00Z","value":91.5,\
                "injectedAnomaly":true}""");

        assertThat(receive().value()).isEqualTo(91.5);
    }

    @Test
    void commitsTheOffsetOnlyAfterTheListenerHasAcknowledged() {
        publish(new DataPoint("a-3", SERIES, TIMESTAMP, 49.0));

        receive();

        await().atMost(RECEIVE_TIMEOUT).untilAsserted(() -> {
            var committed = KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), GROUP, TOPIC, 0);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isPositive();
        });
    }

    private DataPoint receive() {
        var received = ArgumentCaptor.forClass(DataPoint.class);
        verify(listener, timeout(RECEIVE_TIMEOUT.toMillis())).onDataPoint(received.capture(), any());
        return received.getValue();
    }

    private void publish(DataPoint dataPoint) {
        publishRaw(jsonMapper.writeValueAsString(dataPoint));
    }

    private void publishRaw(String payload) {
        var producerFactory = new DefaultKafkaProducerFactory<String, String>(KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer());
        new KafkaTemplate<>(producerFactory).send(TOPIC, SERIES, payload);
    }
}
