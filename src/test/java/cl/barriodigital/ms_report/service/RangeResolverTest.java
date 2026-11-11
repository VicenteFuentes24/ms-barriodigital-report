package cl.barriodigital.ms_report.service;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.exception.InvalidRangeException;
import java.time.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class RangeResolverTest {
    final RangeResolver resolver = new RangeResolver(Clock.fixed(TestEvents.TIME, ZoneOffset.UTC));
    @ParameterizedTest @CsvSource({"last24h,24", "last7d,168", "last30d,720"})
    void computesRelativeRanges(String input, long hours) {
        var range = resolver.resolve(input, null, null, "last24h");
        assertThat(range.to()).isEqualTo(TestEvents.TIME);
        assertThat(range.from()).isEqualTo(TestEvents.TIME.minus(Duration.ofHours(hours)));
    }
    @Test void parsesExplicitInstants() {
        var r = resolver.resolve(null, "2026-11-08T10:00:00Z", "2026-11-08T12:00:00Z", "last7d");
        assertThat(Duration.between(r.from(), r.to())).isEqualTo(Duration.ofHours(2));
    }
    @Test void rejectsUnknownAmbiguousMissingReversedAndOversizedRanges() {
        assertThatThrownBy(() -> resolver.resolve("all", null, null, "last7d")).isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> resolver.resolve("last7d", "2026-11-08T00:00:00Z", "2026-11-09T00:00:00Z", "last7d"))
                .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> resolver.resolve(null, "2026-11-08T00:00:00Z", null, "last7d"))
                .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> resolver.resolve(null, "bad", "also bad", "last7d")).isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> resolver.resolve(null, "2026-11-08T00:00:00Z", "2026-11-08T00:00:00Z", "last7d"))
                .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> resolver.resolve(null, "2026-01-01T00:00:00Z", "2026-11-08T00:00:00Z", "last7d"))
                .isInstanceOf(InvalidRangeException.class);
    }
}
