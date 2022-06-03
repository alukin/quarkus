package io.quarkus.kafka.client.runtime;

import java.util.ArrayList;
import java.util.List;

public class KafkaInfo {
    public String broker = "The broker";
    public List<String> nodes = new ArrayList<>();
    public List<String> topics = new ArrayList<>();
    public List<String> consumerGroups = new ArrayList<>();
    public List<String> producers = new ArrayList<>();
}
