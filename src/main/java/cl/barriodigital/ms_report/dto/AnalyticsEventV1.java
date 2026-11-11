package cl.barriodigital.ms_report.dto;

import java.time.Instant;
import java.util.UUID;

public record AnalyticsEventV1(
        UUID eventId, String eventType, int schemaVersion, UUID sourceEventId,
        EventType sourceEventType, Long requestId, long requestVersion,
        String procedureId, String procedureName, RequestStatus previousStatus,
        RequestStatus status, Instant createdAt, Instant occurredAt, Long resolutionSeconds) { }
