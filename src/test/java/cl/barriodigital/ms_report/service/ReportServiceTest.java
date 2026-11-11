package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.dto.ReportDtos.*;
import cl.barriodigital.ms_report.dto.RequestStatus;
import cl.barriodigital.ms_report.repository.StatisticsRepository;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ReportServiceTest {
    final StatisticsRepository repository = mock(StatisticsRepository.class);
    final ReportService service = new ReportService(repository);
    final TimeRange range = new TimeRange(TestEvents.TIME.minusSeconds(3600), TestEvents.TIME);
    @Test void combinesPersistedAggregates() {
        when(repository.byStatus(range)).thenReturn(Map.of(RequestStatus.RESUELTO, 2L, RequestStatus.INGRESADO, 3L));
        when(repository.averageResolution(range)).thenReturn(1800.0);
        var result = service.kpis(range);
        assertThat(result.totalRequests()).isEqualTo(5);
        assertThat(result.resolvedRequests()).isEqualTo(2);
        assertThat(result.averageResolutionSeconds()).isEqualTo(1800.0);
    }
    @Test void emptyDatasetDoesNotInventAverage() {
        when(repository.byStatus(range)).thenReturn(Map.of());
        when(repository.averageResolution(range)).thenReturn(null);
        assertThat(service.kpis(range).totalRequests()).isZero();
        assertThat(service.kpis(range).averageResolutionSeconds()).isNull();
    }
    @Test void forwardsExactRangeAndLimitToQueries() {
        when(repository.topProcedures(range, 5)).thenReturn(List.of(new ProcedureCount("P", "Procedure", 3)));
        when(repository.byHour(range)).thenReturn(List.of(new HourCount(range.from(), 3)));
        assertThat(service.topProcedures(range, 5).procedures()).hasSize(1);
        assertThat(service.byHour(range).hours()).hasSize(1);
        assertThatThrownBy(() -> service.topProcedures(range, 51)).isInstanceOf(cl.barriodigital.ms_report.exception.InvalidRangeException.class)
                .isNotNull();
    }
}
