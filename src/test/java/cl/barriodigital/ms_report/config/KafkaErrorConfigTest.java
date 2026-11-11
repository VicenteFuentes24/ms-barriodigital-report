package cl.barriodigital.ms_report.config;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import cl.barriodigital.ms_report.service.FailedEventService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.*;
import java.time.Duration;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class KafkaErrorConfigTest {
    final FailedEventService failures = mock(FailedEventService.class);
    @SuppressWarnings("unchecked") final Consumer<String, String> consumer = mock(Consumer.class);
    final MessageListenerContainer container = mock(MessageListenerContainer.class);
    final ConsumerRecord<String, String> record = new ConsumerRecord<>("events", 0, 10, "1", "invalid");

    @Test void commitsRecoveredOffsetOnlyAfterFailureStoreReturns() {
        var handler = new KafkaErrorConfig().kafkaErrorHandler(failures, TestEvents.codec(), 2, 100);
        var properties = new ContainerProperties("events");
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        properties.setSyncCommits(true);
        properties.setSyncCommitTimeout(Duration.ofSeconds(5));
        when(container.getContainerProperties()).thenReturn(properties);
        handler.handleRemaining(new InvalidEventException("invalid"), List.of(record), consumer, container);
        var order = inOrder(failures, consumer);
        order.verify(failures).record(eq(record), any());
        order.verify(consumer).commitSync(eq(Map.of(new TopicPartition("events", 0), new OffsetAndMetadata(11))), any(Duration.class));
    }
    @Test void unavailableFailureStorePreventsOffsetCommit() {
        var handler = new KafkaErrorConfig().kafkaErrorHandler(failures, TestEvents.codec(), 0, 100);
        doThrow(new IllegalStateException("database unavailable")).when(failures).record(any(), any());
        assertThatThrownBy(() -> handler.handleRemaining(new InvalidEventException("invalid"), List.of(record), consumer, container))
                .isInstanceOf(RuntimeException.class).hasMessageContaining("not yet recovered");
        verify(consumer, never()).commitSync(anyMap(), any(Duration.class));
        verify(consumer).seek(new TopicPartition("events", 0), 10L);
    }
    @Test void transientFailureGetsTwoRetriesBeforeQuarantine() {
        var handler = new KafkaErrorConfig().kafkaErrorHandler(failures, TestEvents.codec(), 2, 100);
        when(container.isRunning()).thenReturn(true);
        var failure = new IllegalStateException("transient");
        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        verifyNoInteractions(failures);
        assertThat(handler.handleOne(failure, record, consumer, container)).isTrue();
        verify(failures).record(record, failure);
    }
}
