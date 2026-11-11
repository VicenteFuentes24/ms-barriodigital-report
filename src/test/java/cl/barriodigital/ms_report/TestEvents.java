package cl.barriodigital.ms_report;

import cl.barriodigital.ms_report.dto.*;
import cl.barriodigital.ms_report.kafka.EventCodec;
import jakarta.validation.Validation;
import java.time.Instant;
import java.util.UUID;

public final class TestEvents {
    private TestEvents() { }
    public static final Instant TIME = Instant.parse("2026-11-08T12:00:00Z");
    public static EventCodec codec() {
        return new EventCodec(Validation.buildDefaultValidatorFactory().getValidator());
    }
    public static RequestEventV1 created(long id) {
        return new RequestEventV1(UUID.randomUUID(), EventType.REQUEST_CREATED, 1, id, 1L,
                "P-1", "Permiso municipal", null, RequestStatus.INGRESADO, TIME);
    }
    public static RequestEventV1 changed(long id, long version, RequestStatus previous, RequestStatus status, Instant time) {
        return new RequestEventV1(UUID.randomUUID(), EventType.REQUEST_STATUS_CHANGED, 1, id, version,
                "P-1", "Permiso municipal", previous, status, time);
    }
}
