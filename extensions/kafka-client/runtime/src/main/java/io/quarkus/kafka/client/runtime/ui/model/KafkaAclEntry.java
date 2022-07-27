package io.quarkus.kafka.client.runtime.ui.model;

public class KafkaAclEntry {
    public String operation;
    public String principal;
    public String perm;
    public String pattern;
}
