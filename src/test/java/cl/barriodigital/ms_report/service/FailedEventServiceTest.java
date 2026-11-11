package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.TestEvents;
import java.time.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class FailedEventServiceTest {
    @Test void unavailableFailureStorePropagatesInsteadOfPretendingRecoverySucceeded() {
        var jdbc = mock(JdbcTemplate.class);
        when(jdbc.update(anyString(), any(Object[].class))).thenThrow(new DataAccessResourceFailureException("offline"));
        var service = new FailedEventService(jdbc, TestEvents.codec(), Clock.fixed(TestEvents.TIME, ZoneOffset.UTC));
        assertThatThrownBy(() -> service.record(new ConsumerRecord<>("events", 0, 10, "1", "invalid"), new RuntimeException()))
                .isInstanceOf(DataAccessResourceFailureException.class);
    }
}
