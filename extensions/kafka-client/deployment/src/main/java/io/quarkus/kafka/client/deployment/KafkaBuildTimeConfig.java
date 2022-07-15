package io.quarkus.kafka.client.deployment;

import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

@ConfigRoot(name = "kafka", phase = ConfigPhase.BUILD_TIME)
public class KafkaBuildTimeConfig {
    /**
     * Whether a health check is published in case the smallrye-health extension is present.
     * <p>
     * If you enable the health check, you must specify the `kafka.bootstrap.servers` property.
     */
    @ConfigItem(name = "health.enabled", defaultValue = "false")
    public boolean healthEnabled;

    /**
     * Whether to enable Snappy in native mode.
     * <p>
     * Note that Snappy requires GraalVM 21+ and embeds a native library in the native executable.
     * This library is unpacked and loaded when the application starts.
     */
    @ConfigItem(name = "snappy.enabled", defaultValue = "false")
    public boolean snappyEnabled;

    /**
     * Whether or not to enable Kafka UI in non-development native mode.
     */
    @ConfigItem(name = "ui.enabled", defaultValue = "false")
    public boolean uiEnabled;

    /**
     * Whether or not to enable Kafka Dev UI in non-development native mode.
     */
    @ConfigItem(name = "ui.rootpath", defaultValue = "kafka-ui")
    public String uiRootPath;
    /**
     * Whether or not to enable Kafka Dev UI in non-development native mode.
     */
    @ConfigItem(name = "ui.handlerpath", defaultValue = "kafka-admin")
    public String handlerRootPath;
    /**
     * Whether or not the Kafka is running in RHOSAKe.
     */
    @ConfigItem(name = "rhosak.isUsed", defaultValue = "false")
    public boolean rhosakIsUsed;

    /**
     * Configuration for DevServices. DevServices allows Quarkus to automatically start Kafka in dev and test mode.
     */
    @ConfigItem
    public KafkaDevServicesBuildTimeConfig devservices;
}
