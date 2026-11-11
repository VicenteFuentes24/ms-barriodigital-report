package cl.barriodigital.ms_report.repository;

import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.dto.ReportDtos.*;
import java.sql.Timestamp;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StatisticsRepository {
    private final JdbcTemplate jdbc;
    public StatisticsRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Map<RequestStatus, Long> byStatus(TimeRange range) {
        Map<RequestStatus, Long> result = new EnumMap<>(RequestStatus.class);
        for (var status : RequestStatus.values()) result.put(status, 0L);
        jdbc.query("""
            SELECT status, count(*) AS total FROM bd_report.request_projection
            WHERE created_at >= ? AND created_at < ? GROUP BY status
            """, rs -> { result.put(RequestStatus.valueOf(rs.getString("status")), rs.getLong("total")); },
                Timestamp.from(range.from()), Timestamp.from(range.to()));
        return result;
    }

    public Double averageResolution(TimeRange range) {
        return jdbc.queryForObject("""
            SELECT avg(resolution_seconds) FROM bd_report.request_projection
            WHERE created_at >= ? AND created_at < ? AND status = 'RESUELTO'
            """, Double.class, Timestamp.from(range.from()), Timestamp.from(range.to()));
    }

    public List<ProcedureCount> topProcedures(TimeRange range, int limit) {
        return jdbc.query("""
            SELECT procedure_id, (array_agg(procedure_name ORDER BY last_occurred_at DESC, request_id DESC))[1] AS name,
                   count(*) AS total FROM bd_report.request_projection
            WHERE created_at >= ? AND created_at < ? GROUP BY procedure_id
            ORDER BY total DESC, procedure_id ASC LIMIT ?
            """, (rs, row) -> new ProcedureCount(rs.getString("procedure_id"), rs.getString("name"), rs.getLong("total")),
                Timestamp.from(range.from()), Timestamp.from(range.to()), limit);
    }

    public List<HourCount> byHour(TimeRange range) {
        return jdbc.query("""
            SELECT date_trunc('hour', created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS hour, count(*) AS total
            FROM bd_report.request_projection WHERE created_at >= ? AND created_at < ?
            GROUP BY hour ORDER BY hour
            """, (rs, row) -> new HourCount(rs.getTimestamp("hour").toInstant(), rs.getLong("total")),
                Timestamp.from(range.from()), Timestamp.from(range.to()));
    }
}
