package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.repository.OutboxRepository;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final Clock clock;
    private final String topic;
    private final long timeout;

    public OutboxPublisher(OutboxRepository repository, KafkaTemplate<String, String> kafka, Clock clock,
            @Value("${report.analytics-topic}") String topic,
            @Value("${report.events-topic}") String eventsTopic,
            @Value("${report.outbox.send-timeout-ms}") long timeout) {
        if (topic.equals(eventsTopic)) throw new IllegalArgumentException("Analytics and source topics must differ");
        if (timeout < 1000 || timeout > 60000) throw new IllegalArgumentException("Outbox timeout must be 1000..60000 ms");
        this.repository = repository; this.kafka = kafka; this.clock = clock; this.topic = topic; this.timeout = timeout;
    }

    @Transactional
    public boolean publishNext() {
        var next = repository.nextReady(clock.instant());
        if (next.isEmpty()) return false;
        var row = next.get();
        try {
            kafka.send(topic, row.getRequestId().toString(), row.getPayload()).get(timeout, TimeUnit.MILLISECONDS);
            row.published(clock.instant());
            LOG.atInfo().addKeyValue("eventId", row.getId()).addKeyValue("requestId", row.getRequestId())
                    .log("analytics_published");
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            row.failed(clock.instant(), failure.getClass().getSimpleName());
            LOG.atWarn().addKeyValue("eventId", row.getId()).addKeyValue("requestId", row.getRequestId())
                    .addKeyValue("attempt", row.getAttempts()).addKeyValue("failureType", row.getLastError())
                    .log("analytics_pending_retry");
        }
        return true;
    }
}
