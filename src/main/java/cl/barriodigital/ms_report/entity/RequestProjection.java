package cl.barriodigital.ms_report.entity;

import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import jakarta.persistence.*;
import java.time.Duration;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "request_projection", schema = "bd_report")
@Getter
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class RequestProjection {
    @Id private Long requestId;
    private long requestVersion;
    @Column(length = 100, nullable = false) private String procedureId;
    @Column(length = 250, nullable = false) private String procedureName;
    @Enumerated(EnumType.STRING) @Column(length = 30, nullable = false) private RequestStatus status;
    private Instant createdAt;
    private Instant lastOccurredAt;
    private Instant resolvedAt;
    private Long resolutionSeconds;

    public static RequestProjection create(RequestEventV1 event) {
        if (event.eventType() != EventType.REQUEST_CREATED) {
            throw new InvalidEventException("Missing REQUEST_CREATED: replay source history in requestVersion order");
        }
        RequestProjection result = new RequestProjection();
        result.requestId = event.requestId();
        result.requestVersion = 1;
        result.procedureId = event.procedureId();
        result.procedureName = event.procedureName();
        result.status = event.status();
        result.createdAt = event.occurredAt();
        result.lastOccurredAt = event.occurredAt();
        return result;
    }

    public void apply(RequestEventV1 event) {
        if (event.eventType() != EventType.REQUEST_STATUS_CHANGED || event.requestVersion() != requestVersion + 1) {
            throw new InvalidEventException("Duplicate version, out-of-order event or version gap");
        }
        if (event.previousStatus() != status || !event.procedureId().equals(procedureId)
                || event.occurredAt().isBefore(lastOccurredAt)) {
            throw new InvalidEventException("Previous status, procedure or event time conflicts with projection");
        }
        requestVersion = event.requestVersion();
        procedureName = event.procedureName();
        status = event.status();
        lastOccurredAt = event.occurredAt();
        resolvedAt = status == RequestStatus.RESUELTO ? event.occurredAt() : null;
        resolutionSeconds = resolvedAt == null ? null : Duration.between(createdAt, resolvedAt).getSeconds();
    }
}
