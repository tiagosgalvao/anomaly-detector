package com.anomaly.consumer;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static java.lang.String.*;

@Component
class DetectionReporter {

    static final String LOGGER_NAME = "detection";

    private static final Logger detectionLog = LoggerFactory.getLogger(LOGGER_NAME);

    private static final String NORMAL_LINE = "[%s] Data point: %.2f | Status: OK | Z-score: %.2f";
    private static final String ANOMALY_LINE = "[%s] Data point: %.2f | Status: ANOMALY DETECTED! | Z-score: %.2f | ALERT: Significant deviation detected.";

    @SuppressWarnings("java:S2629")
    void report(Instant observedAt, Detection detection) {
        var template = detection.isAnomaly() ? ANOMALY_LINE : NORMAL_LINE;
        // Locale.ROOT, not the default: a pt-BR or pt-PT machine would render 50,12 rather than 50.12 and
        // silently break the format the brief fixes.
        detectionLog.info(format(Locale.ROOT, template,
                DateTimeFormatter.ISO_INSTANT.format(observedAt.truncatedTo(ChronoUnit.MILLIS)),
                detection.value(),
                detection.zScore()));
    }
}
