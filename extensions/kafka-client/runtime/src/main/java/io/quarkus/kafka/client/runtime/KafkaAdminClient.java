package io.quarkus.kafka.client.runtime;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.*;
import org.jboss.logging.Logger;

import io.quarkus.kafka.client.runtime.ui.model.request.KafkaCreateTopicRequest;
import io.smallrye.common.annotation.Identifier;

@Singleton
public class KafkaAdminClient {

    private static final Logger LOGGER = Logger.getLogger(KafkaAdminClient.class);

    @Inject
    @Identifier("default-kafka-broker")
    private Map<String, Object> config;

    private AdminClient client;

    @PostConstruct
    void init() {
        Map<String, Object> conf = new HashMap<>(config);
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        client = AdminClient.create(conf);
    }

    @PreDestroy
    void stop() {
        client.close();
    }

    public AdminClient getAdminClient() {
        return client;
    }

    public DescribeClusterResult getCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult dcr = client.describeCluster();
        return dcr;
    }

    public Collection<TopicListing> getTopics() throws InterruptedException, ExecutionException {
        return client.listTopics().listings().get();
    }

    public Collection<ConsumerGroupDescription> getConsumerGroups() throws InterruptedException, ExecutionException {
        var consumerGroupIds = client.listConsumerGroups().all().get().stream()
                .map(ConsumerGroupListing::groupId)
                .collect(Collectors.toList());
        return client.describeConsumerGroups(consumerGroupIds).all().get()
                .values();
    }

    public boolean deleteTopic(String name) {
        LOGGER.debug("Deleting kafka topic with ID: " + name);
        Collection<String> topics = new ArrayList<>();
        topics.add(name);
        DeleteTopicsResult dtr = client.deleteTopics(topics);
        return dtr.topicNameValues() != null;
    }

    public boolean createTopic(KafkaCreateTopicRequest kafkaCreateTopicRq) {
        var partitions = Optional.ofNullable(kafkaCreateTopicRq.getPartitions()).orElse(1);
        var replications = Optional.ofNullable(kafkaCreateTopicRq.getReplications()).orElse((short) 1);
        var newTopic = new NewTopic(kafkaCreateTopicRq.getTopicName(), partitions, replications);

        CreateTopicsResult ctr = client.createTopics(List.of(newTopic));
        return ctr.values() != null;
    }

    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
        return getAdminClient().listConsumerGroupOffsets(groupId);
    }
}
