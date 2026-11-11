package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.dto.ReportDtos.*;
import cl.barriodigital.ms_report.dto.RequestStatus;
import cl.barriodigital.ms_report.exception.InvalidRangeException;
import cl.barriodigital.ms_report.repository.StatisticsRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
@Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
public class ReportService {
    private final StatisticsRepository statistics;
    public ReportService(StatisticsRepository statistics) { this.statistics = statistics; }

    public Kpis kpis(TimeRange range) {
        var counts = statistics.byStatus(range);
        long total = counts.values().stream().mapToLong(Long::longValue).sum();
        return new Kpis(range, total, counts, counts.getOrDefault(RequestStatus.RESUELTO, 0L),
                statistics.averageResolution(range));
    }
    public TopProcedures topProcedures(TimeRange range, int limit) {
        if (limit < 1 || limit > 50) throw new InvalidRangeException("limit must be between 1 and 50");
        return new TopProcedures(range, statistics.topProcedures(range, limit));
    }
    public RequestsByHour byHour(TimeRange range) { return new RequestsByHour(range, statistics.byHour(range)); }
}
