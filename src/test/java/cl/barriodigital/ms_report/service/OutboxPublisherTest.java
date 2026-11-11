package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.entity.AnalyticsOutbox;
import cl.barriodigital.ms_report.repository.OutboxRepository;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {
    final OutboxRepository repository = mock(OutboxRepository.class);
    @SuppressWarnings("unchecked") final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    final OutboxPublisher publisher = new OutboxPublisher(repository, kafka, Clock.fixed(TestEvents.TIME, ZoneOffset.UTC),
            "analytics", "events", 1000);
    AnalyticsOutbox row() {
        return new AnalyticsOutbox(UUID.randomUUID(), UUID.randomUUID(), 1L, 1, "{}", TestEvents.TIME);
    }
    @Test void marksPublishedOnlyAfterKafkaAcknowledges() {
        var row = row();
        when(repository.nextReady(TestEvents.TIME)).thenReturn(Optional.of(row));
        when(kafka.send("analytics", "1", "{}")).thenReturn(CompletableFuture.completedFuture(null));
        assertThat(publisher.publishNext()).isTrue();
        assertThat(row.getPublishedAt()).isEqualTo(TestEvents.TIME);
    }
    @Test void retainsUnpublishedRecordWhenKafkaFails() {
        var row = row();
        when(repository.nextReady(TestEvents.TIME)).thenReturn(Optional.of(row));
        when(kafka.send("analytics", "1", "{}")).thenReturn(CompletableFuture.failedFuture(new RuntimeException("unavailable")));
        publisher.publishNext();
        assertThat(row.getPublishedAt()).isNull();
        assertThat(row.getAttempts()).isEqualTo(1);
        assertThat(row.getAvailableAt()).isAfter(TestEvents.TIME);
    }
    @Test void noPendingRowsDoesNotSendAndSameTopicIsForbidden() {
        assertThat(publisher.publishNext()).isFalse();
        verifyNoInteractions(kafka);
        assertThatThrownBy(() -> new OutboxPublisher(repository, kafka, Clock.systemUTC(), "same", "same", 1000))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
