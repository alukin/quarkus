package io.quarkus.kafka.client.runtime;

import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkus.arc.Arc;
import io.quarkus.kafka.client.runtime.devconsole.model.KafkaInfo;

//TODO: remove this, use just POST handler
public class KafkaInfoSupplier implements Supplier<KafkaInfo> {

    private static final Logger LOGGER = Logger.getLogger(KafkaInfoSupplier.class);

    @Override
    public KafkaInfo get() {
        KafkaInfo ki = new KafkaInfo();

        try {
            LOGGER.info("========================= Calling KafkaInfoSupplier ==========================");
            ki = kafkaWebUiUtils().getKafkaInfo();
            LOGGER.info("========================= Call end of KafkaInfoSupplier ==========================");

        } catch (ExecutionException ex) {
            LOGGER.error(ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return ki;
    }

    public static KafkaWebUiUtils kafkaWebUiUtils() {
        return Arc.container().instance(KafkaWebUiUtils.class).get();
    }
}
