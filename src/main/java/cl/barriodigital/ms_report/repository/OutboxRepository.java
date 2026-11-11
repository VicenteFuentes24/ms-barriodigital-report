package cl.barriodigital.ms_report.repository;

import cl.barriodigital.ms_report.entity.AnalyticsOutbox;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<AnalyticsOutbox, UUID> {
    @Query(value = """
        SELECT o.* FROM bd_report.analytics_outbox o
        WHERE o.published_at IS NULL AND o.available_at <= :now
          AND NOT EXISTS (SELECT 1 FROM bd_report.analytics_outbox older
            WHERE older.request_id=o.request_id AND older.published_at IS NULL
              AND older.request_version < o.request_version)
        ORDER BY o.created_at, o.id LIMIT 1 FOR UPDATE OF o SKIP LOCKED
        """, nativeQuery = true)
    Optional<AnalyticsOutbox> nextReady(@Param("now") Instant now);
}
