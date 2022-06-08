package io.quarkus.kafka.client.runtime.devconsole.model;

import java.util.ArrayList;
import java.util.List;

public class ClusterInfo {
    public String id;
    public KafkaNode controller;
    public List<KafkaNode> nodes = new ArrayList<>();
    public String aclOperations;
}
