package io.quarkus.kafka.client.deployment.devconsole;

import static io.quarkus.deployment.annotations.ExecutionTime.STATIC_INIT;

import io.quarkus.deployment.IsDevelopment;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.pkg.builditem.CurateOutcomeBuildItem;
import io.quarkus.devconsole.spi.DevConsoleRouteBuildItem;
import io.quarkus.devconsole.spi.DevConsoleRuntimeTemplateInfoBuildItem;
import io.quarkus.kafka.client.runtime.KafkaInfoSupplier;
import io.quarkus.kafka.client.runtime.devconsole.KafkaDevConsoleRecorder;

public class KafkaDevConsoleProcessor {

    @BuildStep(onlyIf = IsDevelopment.class)
    public DevConsoleRuntimeTemplateInfoBuildItem collectBeanInfo(CurateOutcomeBuildItem curateOutcomeBuildItem) {
        return new DevConsoleRuntimeTemplateInfoBuildItem("kafkaInfos",
                new KafkaInfoSupplier(), this.getClass(), curateOutcomeBuildItem);
    }

    @BuildStep
    @Record(value = STATIC_INIT, optional = true)
    DevConsoleRouteBuildItem invokeEndpoint(KafkaDevConsoleRecorder recorder) {
        return new DevConsoleRouteBuildItem("kafka", "POST", recorder.kafkaControlHandler());
    }
}
