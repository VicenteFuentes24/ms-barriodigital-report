package cl.barriodigital.ms_report.dto;

import java.time.Instant;
import java.util.UUID;
import jakarta.validation.constraints.*;

public record RequestEventV1(
        @NotNull UUID eventId,
        @NotNull EventType eventType,
        @NotNull @Min(1) @Max(1) Integer schemaVersion,
        @NotNull @Positive Long requestId,
        @NotNull @Positive Long requestVersion,
        @NotBlank @Size(max = 100) String procedureId,
        @NotBlank @Size(max = 250) String procedureName,
        RequestStatus previousStatus,
        @NotNull RequestStatus status,
        @NotNull Instant occurredAt) {

    @com.fasterxml.jackson.annotation.JsonIgnore
    @AssertTrue(message = "creation requires version 1, INGRESADO and no previousStatus; changes require version > 1 and different previousStatus")
    public boolean isConsistent() {
        if (eventType == null || requestVersion == null || status == null) return true;
        return switch (eventType) {
            case REQUEST_CREATED -> requestVersion == 1 && previousStatus == null && status == RequestStatus.INGRESADO;
            case REQUEST_STATUS_CHANGED -> requestVersion > 1 && previousStatus != null && previousStatus != status;
        };
    }
}
