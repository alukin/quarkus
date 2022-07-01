package io.quarkus.kafka.client.runtime;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.Arc;
import io.quarkus.kafka.client.runtime.devui.model.KafkaMessageCreateRequest;
import io.quarkus.kafka.client.runtime.devui.model.Order;
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
public class KafkaDevUIRecorder {

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

        return new Handler<RoutingContext>() {

            //This is method that could be copy-patsted to the extension KafkaDevConsoleRecorder.java
            public void handlePost(RoutingContext event) {

                var body = event.body().asJsonObject();
                String action = body.getString("action");
                String key = body.getString("key");
                String value = body.getString("value");
                String message = "OK";

                KafkaDevUiUtils webUtils = kafkaWebUiUtils();
                KafkaAdminClient adminClient = kafkaAdminClient();

                boolean res = false;
                if (null == action) {
                    res = false;
                } else {
                    try {
                        switch (action) {
                            case "getInfo":
                                message = webUtils.toJson(webUtils.getKafkaInfo());
                                res = true;
                                break;
                            case "createTopic":
                                res = adminClient.createTopic(key);
                                message = webUtils.toJson(webUtils.getTopics());
                                break;
                            case "deleteTopic":
                                res = adminClient.deleteTopic(key);
                                message = webUtils.toJson(webUtils.getTopics());
                                break;
                            case "getTopics":
                                message = webUtils.toJson(webUtils.getTopics());
                                res = true;
                                break;
                            case "topicMessages":
                                body.getInteger("");
                                message = webUtils.toJson(webUtils.getTopicMessages(key, Order.OLD_FIRST, List.of(), 0L, 10L));
                                res = true;
                                break;
                            case "createMessage":
                                var mapper = new JsonMapper();
                                var rq = mapper.readValue(event.getBodyAsString(), KafkaMessageCreateRequest.class);
                                webUtils.createMessage(rq);
                                message = "{}";
                                res = true;
                                break;
                            case "getPartitions":
                                var topicName = body.getString("topicName");
                                message = webUtils.toJson(webUtils.partitions(topicName));
                                res = true;
                                break;
                            default:
                                res = false;
                                break;
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException ex) {
                        //  LOGGER.error(ex);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (res) {
                    endResponse(event, OK, message);
                } else {
                    message = "ERROR";
                    endResponse(event, BAD_REQUEST, message);
                }
            }

            private void endResponse(RoutingContext event, HttpResponseStatus status, String message) {
                event.response().setStatusCode(status.code());
                event.response().end(message);
            }

            private KafkaAdminClient kafkaAdminClient() {
                return Arc.container().instance(KafkaAdminClient.class).get();
            }

            private KafkaDevUiUtils kafkaWebUiUtils() {
                return Arc.container().instance(KafkaDevUiUtils.class).get();
            }

            @Override
            public void handle(RoutingContext event) {
                try {
                    if (event.body() != null) {
                        handlePost(event);
                    } else {
                        endResponse(event, BAD_REQUEST, "No POST request body to process");
                    }
                } catch (Exception e) {
                    event.fail(e);
                }
            }
        };
    }

}
