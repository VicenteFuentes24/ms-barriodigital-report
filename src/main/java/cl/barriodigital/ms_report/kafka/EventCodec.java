package cl.barriodigital.ms_report.kafka;

import cl.barriodigital.ms_report.dto.RequestEventV1;
import cl.barriodigital.ms_report.exception.InvalidEventException;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.DeserializationFeature;

@Component
public class EventCodec {
    private final JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
    private final Validator validator;

    public EventCodec(Validator validator) { this.validator = validator; }

    public RequestEventV1 decode(String json, String key) {
        final RequestEventV1 event;
        try {
            if (json == null) throw new IllegalArgumentException();
            event = mapper.readValue(json, RequestEventV1.class);
        } catch (RuntimeException ex) {
            throw new InvalidEventException("Malformed JSON or incompatible event contract");
        }
        if (event == null || !validator.validate(event).isEmpty()) {
            throw new InvalidEventException("Event constraints violated");
        }
        if (!event.requestId().toString().equals(key)) {
            throw new InvalidEventException("Kafka key must equal requestId in decimal form");
        }
        return event;
    }

    public String encode(Object value) { return mapper.writeValueAsString(value); }

    public String identifier(String json, String field) {
        try {
            String value = mapper.readTree(json).path(field).asText("");
            return value.length() <= 64 ? value : "";
        } catch (RuntimeException ex) { return ""; }
    }
}
