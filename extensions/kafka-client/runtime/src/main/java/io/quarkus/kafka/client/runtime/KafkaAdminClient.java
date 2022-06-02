package io.quarkus.kafka.client.runtime;

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
import org.apache.kafka.common.Node;

import io.smallrye.common.annotation.Identifier;

@Singleton
public class KafkaAdminClient {

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

    public Collection<Node> getClusterNodes() throws ExecutionException, InterruptedException {
        return client.describeCluster().nodes().get();
    }
}
