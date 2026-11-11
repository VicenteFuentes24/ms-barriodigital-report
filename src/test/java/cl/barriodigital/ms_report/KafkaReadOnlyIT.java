package cl.barriodigital.ms_report;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Opt-in wire compatibility check. No publishing, topic creation, group joins or offset commits. */
class KafkaReadOnlyIT {
    @Test void managedClientReadsMetadataAndOffsetsFromExistingKafka39Cluster() throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092,localhost:29092,localhost:39092");
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("request.timeout.ms", 10000);
        properties.put("default.api.timeout.ms", 15000);
        List<String> names = List.of("bd.requests.events.v1", "bd.requests.analytics.v1");
        try (var admin = Admin.create(properties)) {
            assertThat(admin.describeCluster().nodes().get(15, TimeUnit.SECONDS)).hasSize(3);
            var topics = admin.describeTopics(names).allTopicNames().get(15, TimeUnit.SECONDS);
            for (String name : names) {
                assertThat(topics.get(name).partitions()).hasSize(3).allSatisfy(partition -> {
                    assertThat(partition.replicas()).hasSize(3);
                    assertThat(partition.isr()).hasSize(3);
                    assertThat(partition.leader().id()).isPositive();
                });
                var resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
                var config = admin.describeConfigs(List.of(resource)).all().get(15, TimeUnit.SECONDS).get(resource);
                assertThat(config.get("min.insync.replicas").value()).isEqualTo("2");
            }
        }
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false);
        properties.put("allow.auto.create.topics", false);
        properties.put("group.protocol", "classic");
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            var partition = new TopicPartition(names.getFirst(), 0);
            consumer.assign(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            consumer.seek(partition, end);
            consumer.poll(Duration.ofMillis(250));
            assertThat(consumer.position(partition)).isGreaterThanOrEqualTo(end);
        }
        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put("bootstrap.servers", bootstrap);
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("max.block.ms", 15000);
        producerProperties.put("enable.idempotence", true);
        producerProperties.put("acks", "all");
        try (var producer = new KafkaProducer<String, String>(producerProperties)) {
            assertThat(producer.partitionsFor(names.get(1))).hasSize(3);
        }
    }
}
