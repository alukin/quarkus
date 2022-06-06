package io.quarkus.kafka.client.runtime.devconsole.model;

public class KafkaTopic {
    public String name;
    public String topicId;
    public boolean internal;

    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" : ").append(topicId);
        return sb.toString();
    }
}
