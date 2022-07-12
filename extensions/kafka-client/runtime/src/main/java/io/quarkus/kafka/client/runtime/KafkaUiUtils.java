package io.quarkus.kafka.client.runtime;

import java.util.*;
import java.util.concurrent.ExecutionException;

import javax.inject.Singleton;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.kafka.client.runtime.converter.KafkaModelConverter;
import io.quarkus.kafka.client.runtime.ui.model.KafkaClusterInfo;
import io.quarkus.kafka.client.runtime.ui.model.KafkaConsumerGroup;
import io.quarkus.kafka.client.runtime.ui.model.KafkaInfo;
import io.quarkus.kafka.client.runtime.ui.model.KafkaMessagePage;
import io.quarkus.kafka.client.runtime.ui.model.KafkaNode;
import io.quarkus.kafka.client.runtime.ui.model.KafkaTopic;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessageCreateRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessagesRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaOffsetRequest;

@Singleton
public class KafkaUiUtils {

    private final KafkaAdminClient kafkaAdminClient;

    private final KafkaTopicClient kafkaTopicClient;
    private final ObjectMapper objectMapper;

    private final KafkaModelConverter modelConverter;

    public KafkaUiUtils(KafkaAdminClient kafkaAdminClient, KafkaTopicClient kafkaTopicClient, ObjectMapper objectMapper,
            KafkaModelConverter modelConverter) {
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaTopicClient = kafkaTopicClient;
        this.objectMapper = objectMapper;
        this.modelConverter = modelConverter;
    }

    public KafkaInfo getKafkaInfo() throws ExecutionException, InterruptedException {
        KafkaInfo ki = new KafkaInfo();
        ki.clusterInfo = getClusterInfo();
        ki.consumerGroups = getConsumerGroups();
        ki.topics = getTopics();
        ki.broker = ki.clusterInfo.controller.host + ":" + ki.clusterInfo.controller.port;
        return ki;
    }

    public List<KafkaTopic> getTopics() throws InterruptedException, ExecutionException {
        List<KafkaTopic> res = new ArrayList<>();
        for (TopicListing tl : kafkaAdminClient.getTopics()) {
            res.add(kafkaTopic(tl));
        }
        return res;
    }

    public long getTopicMessageCount(String topicName) throws ExecutionException, InterruptedException {
        var partitions = kafkaTopicClient.partitions(topicName);
        var maxPartitionOffsetMap = kafkaTopicClient.getPagePartitionOffset(topicName, partitions, Order.NEW_FIRST);
        return maxPartitionOffsetMap.values().stream()
                .reduce(Long::sum)
                .orElse(0L);
    }

    public List<KafkaConsumerGroup> getConsumerGroups() throws InterruptedException, ExecutionException {
        List<KafkaConsumerGroup> res = new ArrayList<>();
        for (ConsumerGroupListing cgl : kafkaAdminClient.getConsumerGroups()) {
            KafkaConsumerGroup cg = new KafkaConsumerGroup();
            cg.name = cgl.groupId();
            cg.state = cgl.state().orElse(ConsumerGroupState.EMPTY).name();
            res.add(cg);
        }
        return res;
    }

    public KafkaClusterInfo getClusterInfo() throws ExecutionException, InterruptedException {
        return clusterInfo(kafkaAdminClient.getCluster());
    }

    public String toJson(Object o) {
        String res;
        try {
            res = objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException ex) {
            //FIXME:
            res = "";
        }
        return res;
    }

    private KafkaTopic kafkaTopic(TopicListing tl) throws ExecutionException, InterruptedException {
        KafkaTopic kt = new KafkaTopic();
        kt.name = tl.name();
        kt.internal = tl.isInternal();
        kt.topicId = tl.topicId().toString();
        kt.nmsg = getTopicMessageCount(kt.name);
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

    public void createMessage(KafkaMessageCreateRequest request) {
        kafkaTopicClient.createMessage(request);
    }

    public Collection<Integer> partitions(String topicName) throws ExecutionException, InterruptedException {
        return kafkaTopicClient.partitions(topicName);
    }

    public Map<Integer, Long> getOffset(KafkaOffsetRequest request) throws ExecutionException, InterruptedException {
        return kafkaTopicClient.getPagePartitionOffset(request.getTopicName(), request.getRequestedPartitions(),
                request.getOrder());
    }

    public KafkaMessagePage getMessages(KafkaMessagesRequest request) throws ExecutionException, InterruptedException {
        return kafkaTopicClient.getTopicMessages(request.getTopicName(), request.getOrder(), request.getPartitionOffset(),
                request.getPageSize());
    }

    public KafkaMessagePage getPage(KafkaMessagesRequest request) throws ExecutionException, InterruptedException {
        return kafkaTopicClient.getPage(request.getTopicName(), request.getOrder(), request.getPageSize(),
                request.getPageNumber(),
                request.getPartitions());
    }
}
