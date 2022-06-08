package io.quarkus.kafka.client.runtime.devconsole.model;

public class ConsumerGroup {
    public String name;
    public String state;
    public String coordinator;
    public String protocol;
    public int members;
    public int lag;
}
