package cl.barriodigital.ms_report.repository;

import cl.barriodigital.ms_report.dto.RequestEventV1;
import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class EventJournal {
    private final JdbcTemplate jdbc;
    public EventJournal(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void lockRequest(long requestId) {
        // Transaction-scoped lock also serializes the FIRST event (no projection row yet).
        jdbc.query("SELECT pg_advisory_xact_lock(hashtextextended('bd_report.request:' || CAST(? AS text), 0))",
                rs -> { }, requestId);
    }

    public boolean insert(RequestEventV1 e, String payload, String hash, String topic, int partition, long offset, Instant now) {
        return jdbc.update("""
            INSERT INTO bd_report.processed_event
              (event_id, request_id, request_version, event_type, occurred_at, processed_at,
               payload, payload_hash, source_topic, source_partition, source_offset)
            VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (event_id) DO NOTHING
            """, e.eventId(), e.requestId(), e.requestVersion(), e.eventType().name(),
                Timestamp.from(e.occurredAt()), Timestamp.from(now), payload, hash, topic, partition, offset) == 1;
    }
}
