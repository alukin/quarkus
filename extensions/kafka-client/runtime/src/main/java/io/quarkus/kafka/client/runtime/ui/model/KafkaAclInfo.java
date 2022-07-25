package io.quarkus.kafka.client.runtime.ui.model;

import java.util.ArrayList;
import java.util.List;

public class KafkaAclInfo {
    public String clusterId;
    public String broker;
    public String aclOperations;
    public List<KafkaAclEntry> entries = new ArrayList<>();
}
