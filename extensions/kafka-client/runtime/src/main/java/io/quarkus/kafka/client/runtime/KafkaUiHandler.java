
package io.quarkus.kafka.client.runtime;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.arc.Arc;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessageCreateRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaMessagesRequest;
import io.quarkus.kafka.client.runtime.ui.model.request.KafkaOffsetRequest;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

public class KafkaUiHandler implements Handler<RoutingContext> {

    //This is method that could be copy-patsted to the extension KafkaDevConsoleRecorder.java
    public void handlePost(RoutingContext event) {

        var body = event.body().asJsonObject();
        String action = body.getString("action");
        String key = body.getString("key");
        String value = body.getString("value");
        String message = "OK";

        KafkaUiUtils webUtils = kafkaWebUiUtils();
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
                        var msgRequest = event.body().asPojo(KafkaMessagesRequest.class);
                        message = webUtils.toJson(webUtils.getMessages(msgRequest));
                        res = true;
                        break;
                    case "createMessage":
                        var mapper = new JsonMapper();
                        var rq = mapper.readValue(event.body().asString(), KafkaMessageCreateRequest.class);
                        webUtils.createMessage(rq);
                        message = "{}";
                        res = true;
                        break;
                    case "getPartitions":
                        var topicName = body.getString("topicName");
                        message = webUtils.toJson(webUtils.partitions(topicName));
                        res = true;
                        break;
                    case "getOffset":
                        var request = event.body().asPojo(KafkaOffsetRequest.class);
                        message = webUtils.toJson(webUtils.getOffset(request));
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

    private KafkaUiUtils kafkaWebUiUtils() {
        return Arc.container().instance(KafkaUiUtils.class).get();
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

}
