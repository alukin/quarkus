package io.quarkus.kafka.client.runtime.ui.model;

import java.util.List;

public class KafkaInfo {
    public String broker;
    public KafkaClusterInfo clusterInfo;
    public List<KafkaTopic> topics;
    public List<KafkaConsumerGroup> consumerGroups;
    public List<String> producers;
}
