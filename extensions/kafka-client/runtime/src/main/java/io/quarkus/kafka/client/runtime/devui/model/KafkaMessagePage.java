package io.quarkus.kafka.client.runtime.devui.model;

import java.util.Collection;
import java.util.Map;

public class KafkaMessagePage {
    private final Map<Integer, Long> partitionOffset;
    private final Collection<KafkaMessage> messages;

    public KafkaMessagePage(Map<Integer, Long> partitionOffset, Collection<KafkaMessage> messages) {
        this.partitionOffset = partitionOffset;
        this.messages = messages;
    }

    public Map<Integer, Long> getPartitionOffset() {
        return partitionOffset;
    }

    public Collection<KafkaMessage> getMessages() {
        return messages;
    }
}
