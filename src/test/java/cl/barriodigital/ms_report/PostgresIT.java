package cl.barriodigital.ms_report;

import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.dto.ReportDtos.*;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import cl.barriodigital.ms_report.kafka.EventCodec;
import cl.barriodigital.ms_report.repository.*;
import cl.barriodigital.ms_report.service.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.beans.factory.annotation.Value;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Opt-in with -Ppostgres-it. Isolated disposable PostgreSQL; never uses DB_URL or Supabase. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.listener.auto-startup=false", "report.outbox.enabled=false",
        "logging.structured.format.console=", "spring.main.banner-mode=off", "logging.level.root=WARN",
        "logging.level.org.springframework=WARN", "logging.level.org.hibernate=WARN", "debug=false"
})
class PostgresIT {
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17.6-alpine")
            .withDatabaseName("report_integration")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(256L * 1024 * 1024));
    static { POSTGRES.start(); }
    @DynamicPropertySource static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
    @AfterAll static void stopDatabase() { POSTGRES.stop(); }
    @Autowired EventProcessingService events;
    @Autowired ReportService reports;
    @Autowired RequestProjectionRepository projections;
    @Autowired OutboxRepository outbox;
    @Autowired OutboxPublisher publisher;
    @Autowired FailedEventService failures;
    @MockitoSpyBean EventCodec codec;
    @Autowired JdbcTemplate jdbc;
    @Value("${local.server.port}") int port;
    @MockitoBean KafkaTemplate<String, String> kafka;

    @BeforeEach void resetIsolatedDatabase() {
        // This database is created exclusively by this test class, not supplied via environment variables.
        jdbc.execute("TRUNCATE bd_report.analytics_outbox, bd_report.processed_event, bd_report.request_projection, bd_report.failed_event RESTART IDENTITY");
    }

    @Test void migrationsIdempotenceProjectionAndRealAggregates() {
        var created = TestEvents.created(1);
        assertThat(events.process(created, "events", 0, 0)).isTrue();
        assertThat(events.process(created, "events", 0, 8)).isFalse();
        events.process(TestEvents.changed(1, 2, RequestStatus.INGRESADO, RequestStatus.RESUELTO,
                TestEvents.TIME.plusSeconds(3600)), "events", 0, 1);
        events.process(TestEvents.created(2), "events", 1, 0);
        var range = new TimeRange(TestEvents.TIME, TestEvents.TIME.plusSeconds(1));
        var kpis = reports.kpis(range);
        assertThat(kpis.totalRequests()).isEqualTo(2);
        assertThat(kpis.resolvedRequests()).isEqualTo(1);
        assertThat(kpis.averageResolutionSeconds()).isEqualTo(3600.0);
        assertThat(kpis.requestsByStatus().get(RequestStatus.INGRESADO)).isEqualTo(1);
        assertThat(reports.topProcedures(range, 10).procedures().getFirst().totalRequests()).isEqualTo(2);
        assertThat(reports.byHour(range).hours()).containsExactly(new HourCount(TestEvents.TIME, 2));
        assertThat(reports.kpis(new TimeRange(TestEvents.TIME.minusSeconds(1), TestEvents.TIME)).totalRequests()).isZero();
        assertThat(outbox.count()).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.processed_event", Long.class)).isEqualTo(3);
    }

    @Test void rollbackDoesNotLeaveEventProjectionOrOutbox() {
        events.process(TestEvents.created(1), "events", 0, 0);
        var gap = TestEvents.changed(1, 3, RequestStatus.INGRESADO, RequestStatus.ADMITIDO, TestEvents.TIME);
        assertThatThrownBy(() -> events.process(gap, "events", 0, 1)).isInstanceOf(InvalidEventException.class);
        assertThat(projections.findById(1L).orElseThrow().getRequestVersion()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.processed_event", Long.class)).isEqualTo(1);
    }

    @Test void concurrentDuplicateCreatesOnlyOneProjectionEventAndOutbox() throws Exception {
        var event = TestEvents.created(5);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var gate = new CountDownLatch(1);
            Callable<Boolean> task = () -> { gate.await(); return events.process(event, "events", 0, 0); };
            var first = pool.submit(task); var second = pool.submit(task); gate.countDown();
            assertThat(List.of(first.get(20, TimeUnit.SECONDS), second.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
        }
        assertThat(projections.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(1);
    }

    @Test void outboxFailureSurvivesAndEarlierVersionBlocksLaterVersion() {
        var e = TestEvents.created(7);
        events.process(e, "events", 0, 0);
        events.process(TestEvents.changed(7, 2, RequestStatus.INGRESADO, RequestStatus.ADMITIDO, TestEvents.TIME), "events", 0, 1);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("offline")));
        assertThat(publisher.publishNext()).isTrue();
        assertThat(publisher.publishNext()).isFalse();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.analytics_outbox WHERE published_at IS NULL", Long.class)).isEqualTo(2);
        jdbc.update("UPDATE bd_report.analytics_outbox SET available_at=? WHERE request_id=7", java.sql.Timestamp.from(Instant.EPOCH));
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        assertThat(publisher.publishNext()).isTrue();
        assertThat(publisher.publishNext()).isTrue();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.analytics_outbox WHERE published_at IS NOT NULL", Long.class)).isEqualTo(2);
    }

    @Test void quarantineIsDurableAndIdempotentForSourceOffset() {
        var record = new ConsumerRecord<>("events", 2, 15L, "8", "{bad json}");
        failures.record(record, new InvalidEventException("invalid"));
        failures.record(record, new InvalidEventException("invalid"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.failed_event", Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT payload FROM bd_report.failed_event", String.class)).isEqualTo("{bad json}");
        assertThat(projections.count()).isZero();
    }

    @Test void outboxInsertFailureRollsBackEventAndProjection() {
        doReturn(null).when(codec).encode(isA(AnalyticsEventV1.class));
        assertThatThrownBy(() -> events.process(TestEvents.created(90), "events", 0, 90))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(projections.count()).isZero();
        assertThat(outbox.count()).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_report.processed_event", Long.class)).isZero();
    }

    @Test void realHttpServerReturnsPersistedKpisAndHealth() throws Exception {
        events.process(TestEvents.created(81), "events", 0, 81);
        try (var client = java.net.http.HttpClient.newHttpClient()) {
            var uri = java.net.URI.create("http://127.0.0.1:" + port + "/api/report/kpis?from=2026-11-08T00:00:00Z&to=2026-11-09T00:00:00Z");
            var result = client.send(java.net.http.HttpRequest.newBuilder(uri).GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(result.statusCode()).isEqualTo(200);
            assertThat(result.body()).contains("\"totalRequests\":1", "\"averageResolutionSeconds\":null");
            var health = client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/actuator/health")).GET().build(), java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(200);
        }
    }
}
