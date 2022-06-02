package io.quarkus.kafka.client.health;

import java.util.concurrent.ExecutionException;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.Node;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import io.quarkus.kafka.client.runtime.KafkaAdminClient;

@Readiness
@ApplicationScoped
public class KafkaHealthCheck implements HealthCheck {

    KafkaAdminClient kafkaAdminClient;

    public KafkaHealthCheck(KafkaAdminClient kafkaAdminClient) {
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public HealthCheckResponse call() {
        if (kafkaAdminClient == null) {
            System.err.println("========== inject does not work =====");
        } else {
            System.err.println("========== inject works =====");
        }
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Kafka connection health check").up();
        try {
            StringBuilder nodes = new StringBuilder();
            for (Node node : kafkaAdminClient.getClusterNodes()) {
                if (nodes.length() > 0) {
                    nodes.append(',');
                }
                nodes.append(node.host()).append(':').append(node.port());
            }
            return builder.withData("nodes", nodes.toString()).build();
        } catch (ExecutionException ex) {
            return builder.down().withData("reason", ex.getMessage()).build();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt(); //TODO: is ti right?
            return builder.down().withData("reason", ex.getMessage()).build();
        }
    }
}
