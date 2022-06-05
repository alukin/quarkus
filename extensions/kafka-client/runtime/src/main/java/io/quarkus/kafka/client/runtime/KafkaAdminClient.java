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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
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

    public Collection<Node> getClusterNodes() throws ExecutionException, InterruptedException {
        return client.describeCluster().nodes().get();
    }

    public Collection<TopicListing> getTopics() throws InterruptedException, ExecutionException {
        return client.listTopics().listings().get();
    }

    public Collection<ConsumerGroupListing> getConsumerGroups() throws InterruptedException, ExecutionException {
        return client.listConsumerGroups().all().get();
    }

    public boolean createTopic(String key) {
        boolean res = true;
        LOGGER.debug("================ Creating topic: " + key);
        ArrayList<NewTopic> newTopics = new ArrayList<>();
        Short n = 1;
        NewTopic nt = new NewTopic(key, 1, n);
        newTopics.add(nt);
        client.createTopics(newTopics);
        return res;
    }
}
