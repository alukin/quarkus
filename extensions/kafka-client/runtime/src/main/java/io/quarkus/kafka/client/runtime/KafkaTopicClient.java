package io.quarkus.kafka.client.runtime;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;

import io.quarkus.kafka.client.runtime.converter.KafkaModelConverter;
import io.quarkus.kafka.client.runtime.ui.model.KafkaMessagePage;
import io.quarkus.kafka.client.runtime.ui.model.Order;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessageCreateRequest;
import io.smallrye.common.annotation.Identifier;

@Singleton
public class KafkaTopicClient {
    // TODO: make configurable
    private static final int RETRIES = 3;

    //TODO: inject me
    private AdminClient adminClient;

    @Inject
    KafkaModelConverter modelConverter;

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
    private Consumer<Bytes, Bytes> createConsumer(String topicName, Integer requestedPartition) {
        Map<String, Object> config = new HashMap<>(this.config);
        //TODO: make generic?
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);

        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-ui-" + UUID.randomUUID());

        // For pagination, we require manual management of offset pointer.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        var consumer = new KafkaConsumer<Bytes, Bytes>(config);
        consumer.assign(List.of(new TopicPartition(topicName, requestedPartition)));
        return consumer;
    }

    private Producer<Bytes, Bytes> createProducer() {
        Map<String, Object> config = new HashMap<>(this.config);

        config.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        // TODO: make generic to support AVRO serializer
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BytesSerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    /**
     * Reads the messages from particular topic.
     *
     * @param topicName topic to read messages from
     * @param order ascending or descending. Defaults to descending (newest first)
     * @param partitionOffsets read offset position per requested partition
     * @return page of messages, matching requested filters
     */
    private KafkaMessagePage getTopicMessages(
            String topicName,
            Order order,
            Map<Integer, Long> partitionOffsets,
            int pageSize,
            int pagesCount)
            throws ExecutionException, InterruptedException {
        //assertParamsValid(pageSize, pagesCount, partitionOffsets);
        var requestedPartitions = partitionOffsets.keySet();
        assertRequestedPartitionsExist(topicName, requestedPartitions);
        if (order == null)
            order = Order.OLD_FIRST;

        //FIXME: last page requesting
        int totalMessages = pageSize * pagesCount;
        var allPartitionsResult = getConsumerRecords(topicName, order, pageSize, requestedPartitions, partitionOffsets,
                pageSize);

        Comparator<ConsumerRecord<Bytes, Bytes>> comparator = Comparator.comparing(ConsumerRecord::timestamp);
        if (Order.NEW_FIRST == order)
            comparator = comparator.reversed();
        allPartitionsResult.sort(comparator);

        // We might have too many values. Throw away newer items, which don't fit into page.
        if (allPartitionsResult.size() > totalMessages) {
            allPartitionsResult = allPartitionsResult.subList(0, totalMessages);
        }

        var newOffsets = calculateNewPartitionOffset(partitionOffsets, allPartitionsResult, order, topicName);
        var convertedResult = allPartitionsResult.stream()
                .map(modelConverter::convert)
                .collect(Collectors.toList());
        return new KafkaMessagePage(newOffsets, convertedResult);
    }

    // Method to fail fast on wrong params, even before querying Kafka.
    private void assertParamsValid(int pageSize, int pagesCount, List<Integer> requestedPartitions,
            Map<Integer, Long> partitionOffsets) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be > 0.");
        if (pagesCount <= 0)
            throw new IllegalArgumentException("Pages count must be > 0.");
        if ((partitionOffsets == null || partitionOffsets.isEmpty()) ||
                (requestedPartitions == null || requestedPartitions.isEmpty()))
            throw new IllegalArgumentException("Either partition map or partition list must be defined.");

        for (var partitionOffset : partitionOffsets.entrySet()) {
            if (partitionOffset.getValue() < 0)
                throw new IllegalArgumentException(
                        "Partition offset must be > 0.");
        }

    }

    public KafkaMessagePage getTopicMessages(String topicName, Order order, Map<Integer, Long> partitionOffsets, int pageSize)
            throws ExecutionException, InterruptedException {
        return getTopicMessages(topicName, order, partitionOffsets, pageSize, 1);
    }

    public KafkaMessagePage getPage(String topicName, Order order, int pageSize, int pageNumber,
            List<Integer> requestedPartitions) throws ExecutionException, InterruptedException {
        var start = getPagePartitionOffset(topicName, requestedPartitions, order);
        return getTopicMessages(topicName, order, start, pageSize, pageNumber);
    }

    private ConsumerRecords<Bytes, Bytes> pollWhenReady(Consumer<Bytes, Bytes> consumer) {
        var attempts = 0;
        ConsumerRecords<Bytes, Bytes> result = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
        while (result.isEmpty() && attempts < RETRIES) {
            result = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            attempts++;
        }
        return result;
    }

    private Map<Integer, Long> calculateNewPartitionOffset(Map<Integer, Long> oldPartitionOffset,
            Collection<ConsumerRecord<Bytes, Bytes>> records, Order order, String topicName) {
        var newOffsets = records.stream().map(ConsumerRecord::partition)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        var newPartitionOffset = new HashMap<Integer, Long>();
        for (var partition : oldPartitionOffset.keySet()) {
            // We should add in case we seek for oldest and reduce for newest.
            var multiplier = Order.OLD_FIRST == order ? 1 : -1;

            // If new offset for partition is not there in the map - we didn't have records for that partition. So, just take the old offset.
            var newOffset = oldPartitionOffset.get(partition) + multiplier * newOffsets.getOrDefault(partition, 0L);
            newPartitionOffset.put(partition, newOffset);
        }
        return newPartitionOffset;
    }

    private long getPosition(String topicName, int partition, Order order) {
        try (var consumer = createConsumer(topicName, partition)) {
            var topicPartition = new TopicPartition(topicName, partition);
            if (Order.NEW_FIRST == order) {
                consumer.seekToEnd(List.of(topicPartition));
            } else {
                consumer.seekToBeginning(List.of(topicPartition));
            }
            return consumer.position(topicPartition);
        }
    }

    public Map<Integer, Long> getPagePartitionOffset(String topicName, List<Integer> requestedPartitions, Order order)
            throws ExecutionException, InterruptedException {
        assertRequestedPartitionsExist(topicName, requestedPartitions);

        var result = new HashMap<Integer, Long>();
        for (var requestedPartition : requestedPartitions) {
            var maxPosition = getPosition(topicName, requestedPartition, order);
            result.put(requestedPartition, maxPosition);
        }

        return result;
    }

    private List<ConsumerRecord<Bytes, Bytes>> getConsumerRecords(String topicName, Order order, int pageSize,
            Collection<Integer> requestedPartitions, Map<Integer, Long> start, int totalMessages) {
        List<ConsumerRecord<Bytes, Bytes>> allPartitionsResult = new ArrayList<>();
        // Requesting a full page from each partition and then filtering out redundant data. Thus, we'll ensure, we read data in historical order.
        for (var requestedPartition : requestedPartitions) {
            List<ConsumerRecord<Bytes, Bytes>> partitionResult = new ArrayList<>();
            var offset = start.get(requestedPartition);
            try (var consumer = createConsumer(topicName, requestedPartition)) {
                // Move pointer to currently read position. It might be different per partition, so requesting with offset per partition.
                var partition = new TopicPartition(topicName, requestedPartition);

                var seekedOffset = Order.OLD_FIRST == order ? offset : Long.max(offset - pageSize, 0);
                consumer.seek(partition, seekedOffset);

                var numberOfMessagesReadSoFar = 0;
                var keepOnReading = true;

                while (keepOnReading) {
                    var records = pollWhenReady(consumer);
                    if (records.isEmpty())
                        keepOnReading = false;

                    for (var record : records) {
                        numberOfMessagesReadSoFar++;
                        partitionResult.add(record);

                        if (numberOfMessagesReadSoFar >= totalMessages) {
                            keepOnReading = false;
                            break;
                        }
                    }
                }
                // We need to cut off result, if it was reset to 0, as we don't want see entries from old pages.
                if (Order.NEW_FIRST == order && seekedOffset == 0) {
                    partitionResult.sort(Comparator.comparing(ConsumerRecord::timestamp));
                    partitionResult = partitionResult.subList(0, offset.intValue());
                }

            }
            allPartitionsResult.addAll(partitionResult);
        }
        return allPartitionsResult;
    }

    private void assertRequestedPartitionsExist(String topicName, Collection<Integer> requestedPartitions)
            throws InterruptedException, ExecutionException {
        var topicPartitions = partitions(topicName);

        if (!new HashSet<>(topicPartitions).containsAll(requestedPartitions)) {
            throw new IllegalArgumentException(String.format(
                    "Requested messages from partition, that do not exist. Requested partitions: %s. Existing partitions: %s",
                    requestedPartitions, topicPartitions));
        }
    }

    public void createMessage(KafkaMessageCreateRequest request) {
        var record = new ProducerRecord<>(request.getTopic(), request.getPartition(), Bytes.wrap(request.getKey().getBytes()),
                Bytes.wrap(request.getValue().getBytes())
        //TODO: support headers
        );

        try (var consumer = createProducer()) {
            consumer.send(record);
        }
    }

    public List<Integer> partitions(String topicName) throws ExecutionException, InterruptedException {
        return adminClient.describeTopics(List.of(topicName)).allTopicNames().get().values().stream().reduce((a, b) -> {
            throw new IllegalStateException("Requested info about single topic, but got result of multiple: " + a + ", " + b);
        }).orElseThrow(
                () -> new IllegalStateException("Requested info about a topic, but nothing found. Topic name: " + topicName))
                .partitions().stream().map(TopicPartitionInfo::partition).collect(Collectors.toList());
    }
}
