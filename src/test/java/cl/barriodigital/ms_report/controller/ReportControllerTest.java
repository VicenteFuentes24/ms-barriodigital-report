package cl.barriodigital.ms_report.controller;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.exception.ApiExceptionHandler;
import cl.barriodigital.ms_report.service.*;
import java.time.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ReportControllerTest {
    @Test void returns400ForInvalidRangeOrLimitTypeAnd405ForWrites() throws Exception {
        var service = mock(ReportService.class);
        var mvc = MockMvcBuilders.standaloneSetup(new ReportController(
                new RangeResolver(Clock.fixed(TestEvents.TIME, ZoneOffset.UTC)), service))
                .setControllerAdvice(new ApiExceptionHandler()).build();
        mvc.perform(get("/api/report/kpis").param("range", "invalid")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/report/top-procedures").param("limit", "abc")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/report/kpis")).andExpect(status().isMethodNotAllowed());
        verifyNoInteractions(service);
    }

    @Test void cannotOpenDatabaseTransactionReturns503WithoutInternalDetails() throws Exception {
        var service = mock(ReportService.class);
        when(service.kpis(any())).thenThrow(new org.springframework.transaction.CannotCreateTransactionException("private database details"));
        var mvc = MockMvcBuilders.standaloneSetup(new ReportController(new RangeResolver(Clock.systemUTC()), service))
                .setControllerAdvice(new ApiExceptionHandler()).build();
        mvc.perform(get("/api/report/kpis")).andExpect(status().isServiceUnavailable())
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("private database details"))));
    }
}
