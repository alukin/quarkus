package io.quarkus.kafka.client.runtime.converter;

import io.quarkus.kafka.client.runtime.ui.model.response.KafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

import java.util.Optional;

public class KafkaModelConverter {
    public KafkaMessage convert(ConsumerRecord<Bytes, Bytes> message) {
        return new KafkaMessage(
                message.topic(),
                message.partition(),
                message.offset(),
                message.timestamp(),
                Optional.ofNullable(message.key()).map(Bytes::toString).orElse(null),
                Optional.ofNullable(message.value()).map(Bytes::toString).orElse(null));
    }
}
