package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.entity.*;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import cl.barriodigital.ms_report.kafka.EventCodec;
import cl.barriodigital.ms_report.repository.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class EventProcessingServiceTest {
    final EventJournal journal = mock(EventJournal.class);
    final ProcessedEventRepository processed = mock(ProcessedEventRepository.class);
    final RequestProjectionRepository projections = mock(RequestProjectionRepository.class);
    final OutboxRepository outbox = mock(OutboxRepository.class);
    final EventCodec codec = TestEvents.codec();
    final EventProcessingService service = new EventProcessingService(journal, processed, projections, outbox,
            codec, Clock.fixed(TestEvents.TIME, ZoneOffset.UTC));

    @BeforeEach void defaults() {
        when(journal.insert(any(), anyString(), anyString(), anyString(), anyInt(), anyLong(), any())).thenReturn(true);
    }
    @Test void persistsCreationAndAnalyticsOutboxTogether() {
        var e = TestEvents.created(1);
        assertThat(service.process(e, "events", 0, 1)).isTrue();
        var projection = ArgumentCaptor.forClass(RequestProjection.class);
        verify(projections).save(projection.capture());
        assertThat(projection.getValue().getStatus()).isEqualTo(RequestStatus.INGRESADO);
        var row = ArgumentCaptor.forClass(AnalyticsOutbox.class);
        verify(outbox).save(row.capture());
        assertThat(row.getValue().getSourceEventId()).isEqualTo(e.eventId());
        assertThat(row.getValue().getPayload()).contains("REQUEST_ANALYTICS_UPDATED");
    }
    @Test void appliesResolutionAndCalculatesDuration() {
        var p = RequestProjection.create(TestEvents.created(1));
        when(projections.findById(1L)).thenReturn(Optional.of(p));
        service.process(TestEvents.changed(1, 2, RequestStatus.INGRESADO, RequestStatus.RESUELTO,
                TestEvents.TIME.plusSeconds(3600)), "events", 0, 2);
        assertThat(p.getResolutionSeconds()).isEqualTo(3600L);
        assertThat(p.getRequestVersion()).isEqualTo(2);
    }
    @Test void duplicateDoesNotChangeProjectionOrProduceAnotherOutbox() throws Exception {
        var e = TestEvents.created(1);
        var stored = mock(ProcessedEvent.class);
        when(stored.getPayloadHash()).thenReturn(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(codec.encode(e).getBytes(StandardCharsets.UTF_8))));
        when(processed.findById(e.eventId())).thenReturn(Optional.of(stored));
        assertThat(service.process(e, "events", 0, 50)).isFalse();
        verifyNoInteractions(projections, outbox);
        verify(journal, never()).insert(any(), any(), any(), any(), anyInt(), anyLong(), any());
    }
    @Test void reusedEventIdIsNotSilentlyIgnored() {
        var e = TestEvents.created(1);
        var stored = mock(ProcessedEvent.class);
        when(stored.getPayloadHash()).thenReturn("different");
        when(processed.findById(e.eventId())).thenReturn(Optional.of(stored));
        assertThatThrownBy(() -> service.process(e, "events", 0, 0)).isInstanceOf(InvalidEventException.class);
    }
    @Test void missingCreationAndVersionGapsAreRejected() {
        var change = TestEvents.changed(1, 3, RequestStatus.INGRESADO, RequestStatus.ADMITIDO, TestEvents.TIME);
        assertThatThrownBy(() -> service.process(change, "events", 0, 0)).isInstanceOf(InvalidEventException.class);
        when(projections.findById(1L)).thenReturn(Optional.of(RequestProjection.create(TestEvents.created(1))));
        assertThatThrownBy(() -> service.process(change, "events", 0, 0)).isInstanceOf(InvalidEventException.class);
        verifyNoInteractions(outbox);
    }
    @Test void wrongPreviousStateAndBackdatedEventAreRejected() {
        var p = RequestProjection.create(TestEvents.created(1));
        assertThatThrownBy(() -> p.apply(TestEvents.changed(1, 2, RequestStatus.ADMITIDO, RequestStatus.RESUELTO, TestEvents.TIME)))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> p.apply(TestEvents.changed(1, 2, RequestStatus.INGRESADO, RequestStatus.ADMITIDO,
                TestEvents.TIME.minusSeconds(1)))).isInstanceOf(InvalidEventException.class);
    }
}
