package io.quarkus.kafka.client.runtime.ui.model;

public class KafkaConsumerGroup {
    public String name;
    public String state;
    public String coordinator;
    public String protocol;
    public int members;
    public int lag;
}
