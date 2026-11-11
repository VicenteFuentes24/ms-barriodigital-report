package cl.barriodigital.ms_report.kafka;

import cl.barriodigital.ms_report.TestEvents;
import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class EventCodecTest {
    private final EventCodec codec = TestEvents.codec();

    @Test void roundTripsCreationAndStatusChange() {
        for (var e : new RequestEventV1[]{TestEvents.created(10),
                TestEvents.changed(10, 2, RequestStatus.INGRESADO, RequestStatus.ADMITIDO, TestEvents.TIME.plusSeconds(60))}) {
            assertThat(codec.decode(codec.encode(e), "10")).isEqualTo(e);
            assertThat(codec.encode(e)).doesNotContain("consistent");
        }
    }
    @Test void documentedExamplesMatchTheActualContract() throws Exception {
        for (String name : new String[]{"request-created.json", "request-status-changed.json"}) {
            String json = java.nio.file.Files.readString(java.nio.file.Path.of("docs/events", name));
            assertThat(codec.decode(json, "900001").requestId()).isEqualTo(900001L);
        }
    }
    @ParameterizedTest @NullAndEmptySource @ValueSource(strings = {"{", "null", "{}", "[]"})
    void rejectsMalformedOrMissingPayload(String payload) {
        assertThatThrownBy(() -> codec.decode(payload, "10")).isInstanceOf(InvalidEventException.class);
    }
    @Test void rejectsWrongKey() {
        assertThatThrownBy(() -> codec.decode(codec.encode(TestEvents.created(10)), "11"))
                .isInstanceOf(InvalidEventException.class).hasMessageContaining("key");
    }
    @ParameterizedTest @ValueSource(strings = {"\"schemaVersion\":2", "\"requestVersion\":0", "\"procedureName\":\"\"", "\"status\":\"UNKNOWN\""})
    void validatesContract(String replacement) {
        String field = replacement.substring(0, replacement.indexOf(':'));
        String payload = codec.encode(TestEvents.created(10)).replaceAll(field + ":[^,}]+", replacement);
        assertThatThrownBy(() -> codec.decode(payload, "10")).isInstanceOf(InvalidEventException.class);
    }
    @Test void rejectsIllegalCreationAndUnknownFields() {
        String payload = codec.encode(TestEvents.created(10));
        assertThatThrownBy(() -> codec.decode(payload.replace("INGRESADO", "RESUELTO"), "10"))
                .isInstanceOf(InvalidEventException.class);
        assertThatThrownBy(() -> codec.decode(payload.replaceFirst("\\{", "{\"extra\":1,"), "10"))
                .isInstanceOf(InvalidEventException.class);
    }
}
