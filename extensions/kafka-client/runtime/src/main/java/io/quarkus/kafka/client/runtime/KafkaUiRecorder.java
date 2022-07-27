package io.quarkus.kafka.client.runtime;

import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.core.Handler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

/**
 * Handles POST requests from dev UI templates
 */
@Recorder
public class KafkaUiRecorder {

    public void setupRoutes(RuntimeValue<Router> routerValue, String prefix) {
        System.out.println("======================== setup of routes in recorder ===================== on " + prefix);
        Router router = routerValue.getValue();
        router.get(prefix + "/*").handler(StaticHandler
                .create("META-INF/resources")
                .setCachingEnabled(false)
                .setDefaultContentEncoding("UTF-8")
        //                .setWebRoot("/")
        );
        router.post(prefix + "/kafka-admin")
                .handler(BodyHandler.create())
                .handler(kafkaControlHandler());
    }

    public Handler<RoutingContext> kafkaControlHandler() {
        return new KafkaUiHandler();
    }
}
