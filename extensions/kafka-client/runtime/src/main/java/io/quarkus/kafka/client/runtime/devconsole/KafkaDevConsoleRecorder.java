package io.quarkus.kafka.client.runtime.devconsole;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
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
                String value = form.get("value");
                KafkaAdminClient adminClient = kafkaAdminClient();
                String message = "OK";
                boolean res = true;
                if ("createTopic".equals(action)) {
                    res = adminClient.createTopic(key);
                } else if ("deleteTopic".equals(action)) {
                    res = adminClient.deleteTopic(key);
                } else if ("topicMesssages".equals(action)) {
                    message = readTopic(key, value);
                } else {
                    res = false;
                }
                if (res) {
                    endResponse(event, OK, message);
                } else {
                    message = "ERROR";
                    endResponse(event, BAD_REQUEST, message);
                }
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

            private String readTopic(String topicName, String offset) {
                return "";
            }
        };
    }

}
