package io.quarkus.kafka.client.runtime;

import static io.quarkus.kafka.client.runtime.util.ConsumerFactory.createConsumer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Singleton;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.quarkus.kafka.client.runtime.ui.model.*;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessageCreateRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessagesRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaOffsetRequest;
import io.smallrye.common.annotation.Identifier;

@Singleton
public class KafkaUiUtils {

    private final KafkaAdminClient kafkaAdminClient;

    private final KafkaTopicClient kafkaTopicClient;
    private final ObjectMapper objectMapper;

    private Map<String, Object> config;

    public KafkaUiUtils(KafkaAdminClient kafkaAdminClient, KafkaTopicClient kafkaTopicClient, ObjectMapper objectMapper,
            @Identifier("default-kafka-broker") Map<String, Object> config) {
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaTopicClient = kafkaTopicClient;
        this.objectMapper = objectMapper;
        this.config = config;
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

    public long getTopicMessageCount(String topicName, List<Integer> partitions)
            throws ExecutionException, InterruptedException {
        var maxPartitionOffsetMap = kafkaTopicClient.getPagePartitionOffset(topicName, partitions, Order.NEW_FIRST);
        return maxPartitionOffsetMap.values().stream()
                .reduce(Long::sum)
                .orElse(0L);
    }

    public List<KafkaConsumerGroup> getConsumerGroups() throws InterruptedException, ExecutionException {
        List<KafkaConsumerGroup> res = new ArrayList<>();
        for (ConsumerGroupDescription cgd : kafkaAdminClient.getConsumerGroups()) {

            var metadata = kafkaAdminClient.listConsumerGroupOffsets(cgd.groupId())
                    .partitionsToOffsetAndMetadata().get();
            var members = cgd.members().stream()
                    .map(member -> new KafkaConsumerGroupMember(
                            member.consumerId(),
                            member.clientId(),
                            member.host(),
                            getPartitionAssignments(metadata, member)))
                    .collect(Collectors.toSet());

            res.add(new KafkaConsumerGroup(
                    cgd.groupId(),
                    cgd.state().name(),
                    cgd.coordinator().host(),
                    cgd.coordinator().id(),
                    cgd.partitionAssignor(),
                    getTotalLag(members),
                    members));
        }
        return res;
    }

    private long getTotalLag(Set<KafkaConsumerGroupMember> members) {
        return members.stream()
                .map(KafkaConsumerGroupMember::getPartitions)
                .flatMap(Collection::stream)
                .map(KafkaConsumerGroupMemberPartitionAssignment::getLag)
                .reduce(Long::sum)
                .orElse(0L);
    }

    private Set<KafkaConsumerGroupMemberPartitionAssignment> getPartitionAssignments(
            Map<TopicPartition, OffsetAndMetadata> topicOffsetMap, MemberDescription member) {
        var topicPartitions = member.assignment().topicPartitions();
        try (var consumer = createConsumer(topicPartitions, config)) {
            var endOffsets = consumer.endOffsets(topicPartitions);

            return topicPartitions.stream()
                    .map(tp -> {
                        var topicOffset = Optional.ofNullable(topicOffsetMap.get(tp))
                                .map(OffsetAndMetadata::offset)
                                .orElse(0L);
                        return new KafkaConsumerGroupMemberPartitionAssignment(tp.partition(), tp.topic(),
                                getLag(topicOffset, endOffsets.get(tp)));
                    })
                    .collect(Collectors.toSet());
        }
    }

    private long getLag(long topicOffset, long endOffset) {
        return endOffset - topicOffset;
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
        var partitions = kafkaTopicClient.partitions(kt.name);
        kt.partitionsCount = partitions.size();
        kt.nmsg = getTopicMessageCount(kt.name, partitions);
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

    public KafkaAclInfo getAclInfo() throws InterruptedException, ExecutionException {
        KafkaAclInfo info = new KafkaAclInfo();
        KafkaClusterInfo ki = clusterInfo(kafkaAdminClient.getCluster());
        info.clusterId = ki.id;
        info.broker = ki.controller.host + ":" + ki.controller.port;
        info.aclOperations = ki.aclOperations;
        try {
            Collection<AclBinding> acls = kafkaAdminClient.getAclInfo();
            for (AclBinding acl : acls) {
                KafkaAclEntry e = new KafkaAclEntry();
                e.operation = acl.entry().operation().name();
                e.principal = acl.entry().principal();
                e.perm = acl.entry().permissionType().name();
                e.pattern = acl.pattern().toString();
                info.entries.add(e);
            }
        } catch (Exception e) {
            info.aclOperations = e.getMessage();
        }
        return info;
    }
}
