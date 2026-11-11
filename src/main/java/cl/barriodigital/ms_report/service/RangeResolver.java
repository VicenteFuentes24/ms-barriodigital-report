package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.dto.ReportDtos.TimeRange;
import cl.barriodigital.ms_report.exception.InvalidRangeException;
import java.time.*;
import org.springframework.stereotype.Component;

@Component
public class RangeResolver {
    private final Clock clock;
    public RangeResolver(Clock clock) { this.clock = clock; }

    public TimeRange resolve(String range, String from, String to, String defaultRange) {
        if (from != null || to != null) {
            if (range != null || from == null || to == null) {
                throw new InvalidRangeException("Use either range or both from and to (ISO-8601 with timezone)");
            }
            try { return validate(Instant.parse(from), Instant.parse(to)); }
            catch (DateTimeException ex) { throw new InvalidRangeException("from and to must be ISO-8601 instants with timezone"); }
        }
        String selected = range == null ? defaultRange : range;
        Duration duration = switch (selected) {
            case "last24h" -> Duration.ofHours(24);
            case "last7d" -> Duration.ofDays(7);
            case "last30d" -> Duration.ofDays(30);
            default -> throw new InvalidRangeException("Supported ranges: last24h, last7d, last30d");
        };
        Instant now = clock.instant();
        return new TimeRange(now.minus(duration), now);
    }

    private TimeRange validate(Instant from, Instant to) {
        if (!from.isBefore(to) || Duration.between(from, to).compareTo(Duration.ofDays(90)) > 0) {
            throw new InvalidRangeException("Range must be nonempty, ascending and at most 90 days");
        }
        return new TimeRange(from, to);
    }
}
