package cl.barriodigital.ms_report.entity;

import jakarta.persistence.*;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "failed_event", schema = "bd_report")
@Getter
@NoArgsConstructor(access = lombok.AccessLevel.PROTECTED)
public class FailedEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(length = 249) private String sourceTopic;
    private int sourcePartition;
    private long sourceOffset;
    @Column(columnDefinition = "text") private String recordKey;
    @Column(length = 64) private String eventId;
    @Column(length = 64) private String requestId;
    @Column(columnDefinition = "text") private String payload;
    @Column(length = 250) private String failureType;
    @Column(length = 500) private String failureReason;
    private Instant failedAt;
}
