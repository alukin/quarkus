package io.quarkus.kafka.client.runtime.devui.model.request;

import java.util.List;

import io.quarkus.kafka.client.runtime.devui.model.Order;

public class KafkaOffsetRequest {
    private String topicName;
    private List<Integer> requestedPartitions;
    private Order order;

    private Integer pageNumber;

    public KafkaOffsetRequest() {
    }

    public KafkaOffsetRequest(String topicName, List<Integer> requestedPartitions, Order order, Integer pageNumber) {
        this.topicName = topicName;
        this.requestedPartitions = requestedPartitions;
        this.order = order;
        this.pageNumber = pageNumber;
    }

    public String getTopicName() {
        return topicName;
    }

    public List<Integer> getRequestedPartitions() {
        return requestedPartitions;
    }

    public Order getOrder() {
        return order;
    }

    public Integer getPageNumber() {
        return pageNumber;
    }
}
