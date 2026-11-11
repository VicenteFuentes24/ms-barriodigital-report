package cl.barriodigital.ms_report.kafka;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.service.EventProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class RequestEventListenerTest {
    @Test void acknowledgesOnlyAfterSuccessfulServiceReturn() {
        var codec = TestEvents.codec();
        var service = mock(EventProcessingService.class);
        var ack = mock(Acknowledgment.class);
        var e = TestEvents.created(1);
        var record = new ConsumerRecord<>("events", 0, 9, "1", codec.encode(e));
        new RequestEventListener(codec, service).consume(record, ack);
        var order = inOrder(service, ack);
        order.verify(service).process(e, "events", 0, 9);
        order.verify(ack).acknowledge();
    }
    @Test void storageFailureDoesNotAcknowledge() {
        var codec = TestEvents.codec();
        var service = mock(EventProcessingService.class);
        var ack = mock(Acknowledgment.class);
        var e = TestEvents.created(1);
        when(service.process(e, "events", 0, 9)).thenThrow(new IllegalStateException("database unavailable"));
        assertThatThrownBy(() -> new RequestEventListener(codec, service)
                .consume(new ConsumerRecord<>("events", 0, 9, "1", codec.encode(e)), ack)).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(ack);
    }
}
