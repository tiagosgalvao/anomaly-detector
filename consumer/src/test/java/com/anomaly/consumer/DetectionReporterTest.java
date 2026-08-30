package com.anomaly.consumer;

import static com.anomaly.consumer.Detection.Status.ANOMALY;
import static com.anomaly.consumer.Detection.Status.NORMAL;
import static com.anomaly.consumer.Detection.Status.WARMING_UP;
import static com.anomaly.consumer.DetectionReporter.LOGGER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class DetectionReporterTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-01-01T12:00:00Z");

    private final DetectionReporter reporter = new DetectionReporter();
    private final ListAppender<ILoggingEvent> captured = new ListAppender<>();

    private ch.qos.logback.classic.Logger detectionLogger;
    private Locale originalLocale;

    @BeforeEach
    void captureDetectionOutput() {
        originalLocale = Locale.getDefault();
        detectionLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(LOGGER_NAME);
        captured.start();
        detectionLogger.addAppender(captured);
    }

    @AfterEach
    void restore() {
        detectionLogger.detachAppender(captured);
        Locale.setDefault(originalLocale);
    }

    @Test
    void writesTheNormalLineExactlyAsTheBriefSpecifiesIt() {
        reporter.report(OBSERVED_AT, new Detection(50.123, 50.0, 5.0, 0.0246, NORMAL));

        assertThat(onlyLine())
                .isEqualTo("[2026-01-01T12:00:00Z] Data point: 50.12 | Status: OK | Z-score: 0.02");
    }

    @Test
    void writesTheAnomalyLineExactlyAsTheBriefSpecifiesIt() {
        reporter.report(OBSERVED_AT, new Detection(91.5, 50.0, 5.0, 8.3, ANOMALY));

        assertThat(onlyLine()).isEqualTo("[2026-01-01T12:00:00Z] Data point: 91.50 "
                + "| Status: ANOMALY DETECTED! | Z-score: 8.30 "
                + "| ALERT: Significant deviation detected.");
    }

    @Test
    void reportsAWarmingUpPointAsNormalSoEveryPointGetsALine() {
        reporter.report(OBSERVED_AT, new Detection(50.0, Double.NaN, Double.NaN, 0.0,
                WARMING_UP));

        assertThat(onlyLine()).isNotEmpty().contains("Status: OK").doesNotContain("NaN");
    }

    @Test
    void usesADecimalPointEvenWhereTheMachineLocaleWouldUseAComma() {
        Locale.setDefault(Locale.forLanguageTag("pt-BR"));

        reporter.report(OBSERVED_AT, new Detection(50.123, 50.0, 5.0, 1.5, NORMAL));

        assertThat(onlyLine()).contains("Data point: 50.12").contains("Z-score: 1.50");
        assertThat(onlyLine()).isNotEmpty().doesNotContain(",");
    }

    @Test
    void alwaysUsesTwoDecimalPlaces() {
        reporter.report(OBSERVED_AT, new Detection(50.0, 50.0, 5.0, 3.0, NORMAL));

        assertThat(onlyLine()).contains("Data point: 50.00").contains("Z-score: 3.00");
    }

    private String onlyLine() {
        return lines().getLast();
    }

    private List<String> lines() {
        return captured.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }
}
