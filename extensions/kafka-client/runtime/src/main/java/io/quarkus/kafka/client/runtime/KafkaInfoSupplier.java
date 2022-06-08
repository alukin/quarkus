package io.quarkus.kafka.client.runtime;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;

import io.quarkus.arc.Arc;
import io.quarkus.kafka.client.runtime.devconsole.model.ConsumerGroup;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaInfo;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaTopic;

public class KafkaInfoSupplier implements Supplier<KafkaInfo> {
    @Override
    public KafkaInfo get() {
        KafkaAdminClient kafkaAdminClient = kafkaAdminClient();
        KafkaInfo ki = new KafkaInfo();

        try {
            for (Node node : kafkaAdminClient.getClusterNodes()) {
                ki.nodes.add(node.toString());
            }
            for (TopicListing tl : kafkaAdminClient.getTopics()) {
                ki.topics.add(kafkaTopic(tl));
            }
            for (ConsumerGroupListing cgl : kafkaAdminClient.getConsumerGroups()) {
                ConsumerGroup cg = new ConsumerGroup();
                cg.name = cgl.groupId();
                cg.state = cgl.state().orElse(ConsumerGroupState.EMPTY).name();
                ki.consumerGroups.add(cg);
            }
        } catch (ExecutionException ex) {
            //log somehow
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return ki;
    }

    private KafkaTopic kafkaTopic(TopicListing tl) {
        KafkaTopic kt = new KafkaTopic();
        kt.name = tl.name();
        kt.internal = tl.isInternal();
        kt.topicId = tl.topicId().toString();
        return kt;
    }

    public static KafkaAdminClient kafkaAdminClient() {
        return Arc.container().instance(KafkaAdminClient.class).get();
    }
}
