package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.kafka.EventCodec;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import java.sql.Timestamp;
import java.time.Clock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class FailedEventService {
    private final JdbcTemplate jdbc;
    private final EventCodec codec;
    private final Clock clock;
    public FailedEventService(JdbcTemplate jdbc, EventCodec codec, Clock clock) {
        this.jdbc = jdbc; this.codec = codec; this.clock = clock;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(ConsumerRecord<?, ?> record, Exception failure) {
        String payload = record.value() == null ? null : record.value().toString();
        String eventId = codec.identifier(payload, "eventId");
        String requestId = codec.identifier(payload, "requestId");
        String type = failure.getClass().getSimpleName();
        if (failure.getCause() != null) type = failure.getCause().getClass().getSimpleName();
        String reason = "Processing failed after retries; inspect service logs using source coordinates";
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof InvalidEventException) { reason = cause.getMessage(); break; }
        }
        jdbc.update("""
            INSERT INTO bd_report.failed_event
            (source_topic,source_partition,source_offset,record_key,event_id,request_id,payload,failure_type,failure_reason,failed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_topic,source_partition,source_offset) DO NOTHING
            """, record.topic(), record.partition(), record.offset(), record.key(), eventId, requestId,
                payload, type, reason, Timestamp.from(clock.instant()));
        // If INSERT or commit fails, recovery throws: DefaultErrorHandler must NOT commit the offset.
    }
}
