package io.quarkus.kafka.client.runtime;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.common.Node;

import io.quarkus.arc.Arc;

public class KafkaInfoSupplier implements Supplier<KafkaInfo> {

    @Override
    public KafkaInfo get() {
        KafkaAdminClient kafkaAdminClient = kafkaAdminClient();
        KafkaInfo ki = new KafkaInfo();

        try {
            for (Node node : kafkaAdminClient.getClusterNodes()) {
                ki.nodes.add(node.toString());
            }
        } catch (ExecutionException ex) {
            //log somehow
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return ki;
    }

    public static KafkaAdminClient kafkaAdminClient() {
        return Arc.container().instance(KafkaAdminClient.class).get();
    }
}
