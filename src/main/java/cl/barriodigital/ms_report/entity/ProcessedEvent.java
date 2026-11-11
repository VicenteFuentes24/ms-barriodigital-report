package cl.barriodigital.ms_report.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "processed_event", schema = "bd_report")
@Getter
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class ProcessedEvent {
    @Id private UUID eventId;
    private Long requestId;
    private long requestVersion;
    @Column(length = 40) private String eventType;
    private Instant occurredAt;
    private Instant processedAt;
    @Column(columnDefinition = "text") private String payload;
    @Column(length = 64) private String payloadHash;
    @Column(length = 249) private String sourceTopic;
    private int sourcePartition;
    private long sourceOffset;
}
