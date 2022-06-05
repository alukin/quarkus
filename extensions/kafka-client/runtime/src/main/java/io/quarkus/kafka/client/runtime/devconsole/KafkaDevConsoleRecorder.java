package io.quarkus.kafka.client.runtime.devconsole;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.Arc;
import io.quarkus.devconsole.runtime.spi.DevConsolePostHandler;
import io.quarkus.kafka.client.runtime.KafkaAdminClient;
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
            protected void handlePost(RoutingContext event, MultiMap form) throws Exception {
                String action = form.get("action");
                String key = form.get("key");
                String data = form.get("value");
                System.err.println("========== POST handler called. Ation: " + action + " key: " + key + " data: " + data);
                performAction(action, key, data);
                endResponse(event, OK, "all good");
            }

            @Override
            protected void actionSuccess(RoutingContext event) {
            }

            private void endResponse(RoutingContext event, HttpResponseStatus status, String message) {
                event.response().setStatusCode(status.code());
                event.response().end(message);
            }

            private KafkaAdminClient kafkaAdminClient() {
                return Arc.container().instance(KafkaAdminClient.class).get();
            }

            private boolean performAction(String action, String key, String data) {
                KafkaAdminClient adminClient = kafkaAdminClient();
                boolean res = true;
                if ("createTopic".equals(action)) {
                    adminClient.createTopic(key);
                }
                return res;
            }
        };
    }

}
