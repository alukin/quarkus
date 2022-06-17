package io.quarkus.kafka.client.runtime.devconsole;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.Arc;
import io.quarkus.devconsole.runtime.spi.DevConsolePostHandler;
import io.quarkus.kafka.client.runtime.KafkaAdminClient;
import io.quarkus.kafka.client.runtime.KafkaWebUiUtils;
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

            ObjectMapper objectMapper = new ObjectMapper();

            @Override
            protected void handlePost(RoutingContext event, MultiMap form) throws Exception {
                String action = form.get("action");
                String key = form.get("key");
                String value = form.get("value");

                KafkaAdminClient adminClient = kafkaAdminClient();
                KafkaWebUiUtils webUtils = kafkaWebUiUtils();
                
                String message = "OK";
                boolean res = true;
                if (null == action) {
                    res = false;
                } else switch (action) {
                    case "createTopic":
                        res = adminClient.createTopic(key);
                        break;
                    case "deleteTopic":
                        res = adminClient.deleteTopic(key);
                        message = webUtils.toJson(webUtils.getTopics());
                        break;
                    case "topicMessages":
                        message = readTopic(key, value);
                        break;
                    default:
                        res = false;
                        break;
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
            
            private KafkaWebUiUtils kafkaWebUiUtils(){
                return Arc.container().instance(KafkaWebUiUtils.class).get();
            }

            private String readTopic(String topicName, String offset) {
                System.out.println(
                        "=============Reading topic: " + topicName + " offset: " + offset + " ObjectMapper " + objectMapper);
                return "";
            }
        };
    }

}
