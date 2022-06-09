package io.quarkus.kafka.client.runtime;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.jboss.logging.Logger;

import io.quarkus.arc.Arc;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaClusterInfo;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaConsumerGroup;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaInfo;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaNode;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaTopic;

public class KafkaInfoSupplier implements Supplier<KafkaInfo> {

    private static final Logger LOGGER = Logger.getLogger(KafkaInfoSupplier.class);

    @Override
    public KafkaInfo get() {
        KafkaAdminClient kafkaAdminClient = kafkaAdminClient();
        KafkaInfo ki = new KafkaInfo();

        try {
            ki.clusterInfo = clusterInfo(kafkaAdminClient.getCluster());
            for (TopicListing tl : kafkaAdminClient.getTopics()) {
                ki.topics.add(kafkaTopic(tl));
            }
            for (ConsumerGroupListing cgl : kafkaAdminClient.getConsumerGroups()) {
                KafkaConsumerGroup cg = new KafkaConsumerGroup();
                cg.name = cgl.groupId();
                cg.state = cgl.state().orElse(ConsumerGroupState.EMPTY).name();
                ki.consumerGroups.add(cg);
            }
        } catch (ExecutionException ex) {
            LOGGER.error("Error getting Kafka cluster info", ex);
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

    private KafkaNode kafkaNode(Node n) {
        KafkaNode kn = new KafkaNode();
        kn.host = n.host();
        kn.id = n.idString();
        kn.port = n.port();
        return kn;
    }

    private KafkaClusterInfo clusterInfo(DescribeClusterResult dcr) throws InterruptedException, ExecutionException {
        KafkaClusterInfo ci = new KafkaClusterInfo();
        ci.id = dcr.clusterId().get();
        ci.controller = kafkaNode(dcr.controller().get());
        for (Node n : dcr.nodes().get()) {
            ci.nodes.add(kafkaNode(n));
        }
        Set<AclOperation> ops = dcr.authorizedOperations().get();
        if (ops != null) {
            for (AclOperation op : dcr.authorizedOperations().get()) {
                if (ci.aclOperations.isEmpty()) {
                    ci.aclOperations += ", ";
                }
                ci.aclOperations += op.name();
            }
        } else {
            ci.aclOperations = "NONE";
        }
        return ci;
    }

    public static KafkaAdminClient kafkaAdminClient() {
        return Arc.container().instance(KafkaAdminClient.class).get();
    }
}
