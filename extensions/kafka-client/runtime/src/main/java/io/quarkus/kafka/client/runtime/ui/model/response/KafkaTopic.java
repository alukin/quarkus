package io.quarkus.kafka.client.runtime.ui.model.response;

public class KafkaTopic {
    public String name;
    public String topicId;
    public int partitionsCount;
    public boolean internal;
    public long nmsg = 0;

    public String toString() {
        StringBuilder sb = new StringBuilder(name);
        sb.append(" : ").append(topicId);
        return sb.toString();
    }
}
