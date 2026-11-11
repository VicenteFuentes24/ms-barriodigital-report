package cl.barriodigital.ms_report.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "analytics_outbox", schema = "bd_report")
@Getter
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class AnalyticsOutbox {
    @Id private UUID id;
    private UUID sourceEventId;
    private Long requestId;
    private long requestVersion;
    @Column(columnDefinition = "text") private String payload;
    private Instant createdAt;
    private Instant availableAt;
    private Instant publishedAt;
    private int attempts;
    @Column(length = 250) private String lastError;

    public AnalyticsOutbox(UUID id, UUID sourceEventId, Long requestId, long requestVersion, String payload, Instant now) {
        this.id = id; this.sourceEventId = sourceEventId; this.requestId = requestId;
        this.requestVersion = requestVersion; this.payload = payload;
        this.createdAt = now; this.availableAt = now;
    }
    public void published(Instant now) { publishedAt = now; lastError = null; }
    public void failed(Instant now, String failureType) {
        attempts++;
        availableAt = now.plusSeconds(Math.min(60, 1L << Math.min(attempts, 6)));
        lastError = failureType;
    }
}
