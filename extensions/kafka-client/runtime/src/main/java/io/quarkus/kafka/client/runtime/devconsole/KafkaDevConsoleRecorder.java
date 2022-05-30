package io.quarkus.kafka.client.runtime.devconsole;

import io.quarkus.devconsole.runtime.spi.DevConsolePostHandler;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;

/**
 * Handles POST requests from dev UI templates
 */
@Recorder
public class KafkaDevConsoleRecorder {
    public Handler<RoutingContext> kafkaControlHandler() {
        return new DevConsolePostHandler() {
            @Override
            protected void handlePost(io.vertx.ext.web.RoutingContext event, MultiMap form) {

            }

            @Override
            protected void actionSuccess(RoutingContext event) {
            }
        };
    }
}
