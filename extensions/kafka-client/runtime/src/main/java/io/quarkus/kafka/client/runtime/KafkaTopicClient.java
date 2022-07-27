package io.quarkus.kafka.client.runtime;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.jboss.logging.Logger;

import io.quarkus.kafka.client.runtime.devconsole.model.KafkaMessageCreateRequest;
import io.quarkus.kafka.client.runtime.devconsole.model.Order;
import io.smallrye.common.annotation.Identifier;

@Singleton
public class KafkaTopicClient {

    private static final Logger LOGGER = Logger.getLogger(KafkaTopicClient.class);

    private AdminClient adminClient;

    @Inject
    @Identifier("default-kafka-broker")
    Map<String, Object> config;

    @PostConstruct
    void init() {
        Map<String, Object> conf = new HashMap<>(config);
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        adminClient = AdminClient.create(conf);
    }

    // We must create a new instance per request, as we might have multiple windows open, each with different pagination, filter and thus different cursor.
    private Consumer<Bytes, Bytes> createConsumer(String topicName, List<Integer> requestedPartitions)
            throws ExecutionException, InterruptedException {
        Map<String, Object> config = new HashMap<>(this.config);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-ui-" + UUID.randomUUID());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        // For pagination, we require manual management of offset pointer.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var consumer = new KafkaConsumer<Bytes, Bytes>(config);
        // FIXME: use local copy
        if (requestedPartitions.isEmpty())
            requestedPartitions = partitions(topicName);
        consumer.assign(requestedPartitions.stream()
                .map(p -> new TopicPartition(topicName, p))
                .collect(Collectors.toList()));
        return consumer;
    }

    private Producer<Bytes, Bytes> createProducer() {
        Map<String, Object> config = new HashMap<>(this.config);

        config.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                BytesSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                BytesSerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    /**
     * Reads the messages from particular topic.
     *
     * @param topicId topic to read messages from
     * @param order ascending or descending. Defaults to descending (newest first)
     * @param requestedPartitions partitions to show the messages for. Empty list means request messages from all partitions.
     * @param offset read offset position
     * @param pageSizePerPartition size of a page per partition
     * @return messages, that match requested filters
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Collection<ConsumerRecord<Bytes, Bytes>> getTopicMessages(String topicId, Order order,
            List<Integer> requestedPartitions, long offset,
            long pageSizePerPartition) throws ExecutionException, InterruptedException {
        assertRequestedPartitionsExist(topicId, requestedPartitions);

        var result = new ArrayList<ConsumerRecord<Bytes, Bytes>>();
        try (var consumer = createConsumer(topicId, requestedPartitions)) {
            // Move pointer to currently read position.
            for (var requestedPartition : requestedPartitions) {
                var partition = new TopicPartition(topicId, requestedPartition);
                consumer.seek(partition, offset);
            }

            var numberOfMessagesReadSoFar = 0;
            var keepOnReading = true;

            while (keepOnReading) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty())
                    keepOnReading = false;

                for (var record : records) {
                    numberOfMessagesReadSoFar++;
                    LOGGER.info("Key: " + record.key() + ", Value: " + record.value());
                    LOGGER.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    if (numberOfMessagesReadSoFar >= pageSizePerPartition) {
                        keepOnReading = false;
                        break;
                    }
                    result.add(record);
                }
            }
        }
        return result;
    }

    private void assertRequestedPartitionsExist(String topicName, List<Integer> requestedPartitions)
            throws InterruptedException, ExecutionException {
        if (requestedPartitions.isEmpty())
            return;
        var topicPartitions = partitions(topicName);

        if (!new HashSet<>(topicPartitions).containsAll(requestedPartitions)) {
            throw new IllegalArgumentException(String.format(
                    "Requested messages from partition, that do not exist. Requested partitions: %s. Existing partitions: %s",
                    requestedPartitions, topicPartitions));
        }
    }

    public void createMessage(KafkaMessageCreateRequest request) {
        var record = new ProducerRecord<>(
                request.getTopic(),
                request.getPartition(),
                Bytes.wrap(request.getKey().getBytes()),
                Bytes.wrap(request.getValue().getBytes())//,
        //TODO: support headers
        //request.getHeaders
        );

        try (var consumer = createProducer()) {
            consumer.send(record);
        }
    }

    public List<Integer> partitions(String topicName) throws ExecutionException, InterruptedException {
        return adminClient.describeTopics(List.of(topicName))
                .allTopicNames()
                .get().values().stream()
                .reduce((a, b) -> {
                    throw new IllegalStateException(
                            "Requested info about single topic, but got result of multiple: " + a + ", " + b);
                })
                .orElseThrow(() -> new IllegalStateException(
                        "Requested info about a topic, but nothing found. Topic name: " + topicName))
                .partitions().stream()
                .map(TopicPartitionInfo::partition)
                .collect(Collectors.toList());
    }
}
