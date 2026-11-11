package cl.barriodigital.ms_report.config;

import cl.barriodigital.ms_report.exception.InvalidEventException;
import cl.barriodigital.ms_report.service.FailedEventService;
import cl.barriodigital.ms_report.kafka.EventCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaErrorConfig.class);
    @Bean
    DefaultErrorHandler kafkaErrorHandler(FailedEventService failures, EventCodec codec,
            @Value("${report.consumer-retries}") long retries,
            @Value("${report.retry-delay-ms}") long delay) {
        if (retries < 0 || retries > 10 || delay < 100 || delay > 10000) {
            throw new IllegalArgumentException("Consumer retries must be 0..10 and delay 100..10000 ms");
        }
        var handler = new DefaultErrorHandler((record, error) -> {
            failures.record(record, error);
            // The failure-store transaction has committed at this point.
            String payload = record.value() == null ? null : record.value().toString();
            LOG.atError().addKeyValue("eventId", codec.identifier(payload, "eventId"))
                    .addKeyValue("requestId", codec.identifier(payload, "requestId"))
                    .addKeyValue("topic", record.topic()).addKeyValue("partition", record.partition())
                    .addKeyValue("offset", record.offset()).log("event_quarantined");
        }, new FixedBackOff(delay, retries));
        handler.setRetryListeners((record, error, attempt) -> {
            String payload = record.value() == null ? null : record.value().toString();
            LOG.atWarn().addKeyValue("eventId", codec.identifier(payload, "eventId"))
                    .addKeyValue("requestId", codec.identifier(payload, "requestId"))
                    .addKeyValue("partition", record.partition()).addKeyValue("offset", record.offset())
                    .addKeyValue("attempt", attempt).addKeyValue("failureType", error.getClass().getSimpleName())
                    .log("event_processing_failed");
        });
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setCommitRecovered(true);
        handler.setResetStateOnRecoveryFailure(true);
        return handler;
    }
}
