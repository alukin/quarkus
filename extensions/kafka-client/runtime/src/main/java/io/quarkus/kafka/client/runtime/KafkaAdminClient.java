package io.quarkus.kafka.client.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.jboss.logging.Logger;

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

    public Collection<ConsumerGroupListing> getConsumerGroups() throws InterruptedException, ExecutionException {
        return client.listConsumerGroups().all().get();
    }

    public Collection<String> getTopicMessages(String topicId) {
        ArrayList<String> result = new ArrayList<>();
        //TODO: pool specific topic
        return result;
    }

    public boolean deleteTopic(String name) {
        LOGGER.debug("Dleting kafka topic with ID: " + name);
        Collection<String> topics = new ArrayList<>();
        topics.add(name);
        client.deleteTopics(topics);
        return false;
    }

    public boolean createTopic(String name) {
        LOGGER.debug("Creating kafka topic: " + name);
        ArrayList<NewTopic> newTopics = new ArrayList<>();
        NewTopic nt = new NewTopic(name, 1, (short) 1);
        newTopics.add(nt);
        CreateTopicsResult tr = client.createTopics(newTopics);
        boolean res = true;
        try {
            res = tr.all().get() != null;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ex) {
            LOGGER.warn("Can not create Kafka topic {}", name, ex);
            res = false;
        }
        return res;
    }
}
