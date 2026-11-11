package cl.barriodigital.ms_report.controller;

import cl.barriodigital.ms_report.dto.ReportDtos.*;
import cl.barriodigital.ms_report.service.*;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/report")
public class ReportController {
    private final RangeResolver ranges;
    private final ReportService reports;
    public ReportController(RangeResolver ranges, ReportService reports) { this.ranges = ranges; this.reports = reports; }

    @GetMapping("/kpis")
    public Kpis kpis(@RequestParam(required = false) String range,
            @RequestParam(required = false) String from, @RequestParam(required = false) String to) {
        return reports.kpis(ranges.resolve(range, from, to, "last24h"));
    }
    @GetMapping("/top-procedures")
    public TopProcedures top(@RequestParam(required = false) String range,
            @RequestParam(required = false) String from, @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "10") int limit) {
        return reports.topProcedures(ranges.resolve(range, from, to, "last7d"), limit);
    }
    @GetMapping("/requests-by-hour")
    public RequestsByHour byHour(@RequestParam(required = false) String range,
            @RequestParam(required = false) String from, @RequestParam(required = false) String to) {
        return reports.byHour(ranges.resolve(range, from, to, "last24h"));
    }
}
