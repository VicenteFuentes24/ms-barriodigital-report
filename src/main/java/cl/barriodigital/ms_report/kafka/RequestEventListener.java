package cl.barriodigital.ms_report.kafka;

import cl.barriodigital.ms_report.service.EventProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RequestEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(RequestEventListener.class);
    private final EventCodec codec;
    private final EventProcessingService service;
    public RequestEventListener(EventCodec codec, EventProcessingService service) {
        this.codec = codec; this.service = service;
    }

    @KafkaListener(topics = "${report.events-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        var event = codec.decode(record.value(), record.key());
        // The proxied service transaction commits BEFORE this method acknowledges Kafka.
        boolean applied = service.process(event, record.topic(), record.partition(), record.offset());
        acknowledgment.acknowledge();
        LOG.atInfo().addKeyValue("eventId", event.eventId()).addKeyValue("requestId", event.requestId())
                .addKeyValue("partition", record.partition()).addKeyValue("offset", record.offset())
                .addKeyValue("applied", applied).log("request_event_processed");
    }
}
