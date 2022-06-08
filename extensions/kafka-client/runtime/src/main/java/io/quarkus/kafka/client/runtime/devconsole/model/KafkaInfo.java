package io.quarkus.kafka.client.runtime.devconsole.model;

import java.util.ArrayList;
import java.util.List;

public class KafkaInfo {
    public String broker = "The broker";
    public ClusterInfo clusterInfo;
    public List<KafkaTopic> topics = new ArrayList<>();
    public List<ConsumerGroup> consumerGroups = new ArrayList<>();
    public List<String> producers = new ArrayList<>();
}
