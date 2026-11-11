package cl.barriodigital.ms_report.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public final class ReportDtos {
    private ReportDtos() { }
    public record TimeRange(Instant from, Instant to) { }
    public record Kpis(TimeRange range, long totalRequests, Map<RequestStatus, Long> requestsByStatus,
                       long resolvedRequests, Double averageResolutionSeconds) { }
    public record ProcedureCount(String procedureId, String procedureName, long totalRequests) { }
    public record TopProcedures(TimeRange range, List<ProcedureCount> procedures) { }
    public record HourCount(Instant hour, long totalRequests) { }
    public record RequestsByHour(TimeRange range, List<HourCount> hours) { }
}
