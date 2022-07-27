package io.quarkus.kafka.client.runtime.ui.model;

import java.util.ArrayList;
import java.util.List;

public class KafkaClusterInfo {
    public String id;
    public KafkaNode controller;
    public List<KafkaNode> nodes = new ArrayList<>();
    public String aclOperations;
}
