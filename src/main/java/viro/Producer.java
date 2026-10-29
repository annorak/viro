package viro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public final class Producer {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "viro-observations");
        String sourceFile = System.getenv().getOrDefault("SOURCE_FILE", "/source/observations.json");
        int rate = Integer.parseInt(System.getenv().getOrDefault("RATE_PER_SECOND", "200"));
        int repeats = Integer.parseInt(System.getenv().getOrDefault("REPEATS", "1"));
        if (rate < 0 || repeats < 1) throw new IllegalArgumentException("Invalid rate or repeat count");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rows = mapper.readTree(Path.of(sourceFile).toFile());
        if (!rows.isArray() || rows.isEmpty()) throw new IllegalArgumentException("Source must be a nonempty JSON array");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(properties)) {
            try {
                admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1))).all().get(30, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                if (!(error.getCause() instanceof TopicExistsException)) throw error;
            }
            int partitions = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS)
                    .get(topic).partitions().size();
            if (partitions != 3) throw new IllegalStateException("Viro requires exactly three partitions");
        }
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        properties.put("enable.idempotence", "true");
        properties.put("acks", "all");
        properties.put("delivery.timeout.ms", "60000");
        properties.put("request.timeout.ms", "10000");
        properties.put("max.block.ms", "30000");
        properties.put("buffer.memory", "8388608");
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        int[] partitionCounts = new int[3];
        long started = System.nanoTime();
        long next = started;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int pass = 0; pass < repeats; pass++) {
                for (JsonNode row : rows) {
                    if (failure.get() != null) throw failure.get();
                    String key = required(row, "site") + "|" + required(row, "pathogen_target") + "|" + required(row, "week_end");
                    int partition = Math.floorMod(key.hashCode(), 3);
                    if (rate > 0) {
                        while (System.nanoTime() < next) {
                            LockSupport.parkNanos(next - System.nanoTime());
                            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Publication interrupted");
                        }
                        next += 1_000_000_000L / rate;
                    }
                    ObjectNode event = ((ObjectNode) row).deepCopy();
                    event.put("viro_published_at_ms", System.currentTimeMillis());
                    producer.send(new ProducerRecord<>(topic, partition, key, mapper.writeValueAsString(event)),
                            (metadata, error) -> {
                                if (error == null) acknowledged.incrementAndGet();
                                else failure.compareAndSet(null, error);
                            });
                    partitionCounts[partition]++;
                }
            }
            producer.flush();
            if (failure.get() != null) throw failure.get();
        }
        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        System.out.println(mapper.writeValueAsString(java.util.Map.of(
                "event", "publication_complete", "source_rows", rows.size(), "repeats", repeats,
                "acknowledged", acknowledged.get(), "seconds", seconds,
                "acknowledged_per_second", acknowledged.get() / seconds,
                "partition_counts", partitionCounts)));
    }

    private static String required(JsonNode row, String field) {
        JsonNode value = row.get(field);
        if (value == null || value.asText().isBlank()) throw new IllegalArgumentException("Missing " + field);
        return value.asText();
    }
}
