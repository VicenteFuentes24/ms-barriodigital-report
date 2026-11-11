package cl.barriodigital.ms_report.kafka;

import cl.barriodigital.ms_report.service.OutboxPublisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "report.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private final OutboxPublisher publisher;
    private final int batch;
    public OutboxScheduler(OutboxPublisher publisher, @Value("${report.outbox.batch-size}") int batch) {
        if (batch < 1 || batch > 100) throw new IllegalArgumentException("Outbox batch must be 1..100");
        this.publisher = publisher; this.batch = batch;
    }
    @Scheduled(fixedDelayString = "${report.outbox.delay-ms}")
    public void publish() {
        for (int i = 0; i < batch && !Thread.currentThread().isInterrupted(); i++) {
            if (!publisher.publishNext()) break;
        }
    }
}
