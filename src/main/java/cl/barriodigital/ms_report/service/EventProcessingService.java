package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.entity.*;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import cl.barriodigital.ms_report.kafka.EventCodec;
import cl.barriodigital.ms_report.repository.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProcessingService {
    private final EventJournal journal;
    private final ProcessedEventRepository processed;
    private final RequestProjectionRepository projections;
    private final OutboxRepository outbox;
    private final EventCodec codec;
    private final Clock clock;

    public EventProcessingService(EventJournal journal, ProcessedEventRepository processed,
            RequestProjectionRepository projections, OutboxRepository outbox, EventCodec codec, Clock clock) {
        this.journal = journal; this.processed = processed; this.projections = projections;
        this.outbox = outbox; this.codec = codec; this.clock = clock;
    }

    @Transactional
    public boolean process(RequestEventV1 e, String topic, int partition, long offset) {
        journal.lockRequest(e.requestId());
        String payload = codec.encode(e);
        String hash = hash(payload);
        var previous = processed.findById(e.eventId());
        if (previous.isPresent()) return duplicate(previous.get(), hash);
        var current = projections.findById(e.requestId());
        RequestProjection projection;
        if (current.isPresent()) {
            projection = current.get();
            projection.apply(e);
        } else {
            projection = RequestProjection.create(e);
        }
        if (!journal.insert(e, payload, hash, topic, partition, offset, clock.instant())) {
            // An eventId collision across different request IDs: compare rather than silently discard.
            throw new InvalidEventException("eventId concurrently reused across requests");
        }
        projections.save(projection);
        UUID analyticsId = UUID.nameUUIDFromBytes(("bd-report-v1:" + e.eventId()).getBytes(StandardCharsets.UTF_8));
        var analytics = new AnalyticsEventV1(analyticsId, "REQUEST_ANALYTICS_UPDATED", 1, e.eventId(),
                e.eventType(), e.requestId(), e.requestVersion(), projection.getProcedureId(),
                projection.getProcedureName(), e.previousStatus(), e.status(), projection.getCreatedAt(),
                e.occurredAt(), projection.getResolutionSeconds());
        outbox.save(new AnalyticsOutbox(analyticsId, e.eventId(), e.requestId(), e.requestVersion(),
                codec.encode(analytics), clock.instant()));
        return true;
    }

    private boolean duplicate(ProcessedEvent stored, String hash) {
        if (!hash.equals(stored.getPayloadHash())) throw new InvalidEventException("eventId reused with different content");
        return false;
    }

    private String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) { throw new IllegalStateException(ex); }
    }
}
