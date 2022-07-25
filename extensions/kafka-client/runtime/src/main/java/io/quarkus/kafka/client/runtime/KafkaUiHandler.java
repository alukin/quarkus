
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
import io.quarkus.security.identity.CurrentIdentityAssociation;
import io.quarkus.vertx.http.runtime.CurrentVertxRequest;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

public class KafkaUiHandler extends AbstractHttpRequestHandler {

    public KafkaUiHandler(CurrentIdentityAssociation currentIdentityAssociation, CurrentVertxRequest currentVertxRequest) {
        super(currentIdentityAssociation, currentVertxRequest);
    }

    //This is method that could be copy-patsted to the extension KafkaDevConsoleRecorder.java
    @Override
    public void handlePost(RoutingContext event) {
        if (event.body() == null) {
            endResponse(event, BAD_REQUEST, "Request body is null");
            return;
        }
        var body = event.body().asJsonObject();
        if (body == null) {
            endResponse(event, BAD_REQUEST, "Request JSON body is null");
            return;
        }
        String action = body.getString("action");
        String key = body.getString("key");
        String value = body.getString("value");
        String message = "OK";
        String error = "";

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
                    case "getAclInfo":
                        message = webUtils.toJson(webUtils.getAclInfo());
                        res = true;
                        break;
                    case "createTopic":
                        res = adminClient.createTopic(key);
                        message = webUtils.toJson(webUtils.getTopics());
                        break;
                    case "deleteTopic":
                        res = adminClient.deleteTopic(key);
                        //                        message = webUtils.toJson(webUtils.getTopics());
                        message = "{}";
                        res = true;
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
                    case "getOffset":
                        var request = event.body().asPojo(KafkaOffsetRequest.class);
                        message = webUtils.toJson(webUtils.getOffset(request));
                        res = true;
                        break;
                    case "getPage":
                        var msRequest = event.body().asPojo(KafkaMessagesRequest.class);
                        message = webUtils.toJson(webUtils.getPage(msRequest));
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
            } catch (ExecutionException | JsonProcessingException ex) {
                throw new RuntimeException(ex);
            }
        }

        if (res) {
            endResponse(event, OK, message);
        } else {
            message = "ERROR: " + error;
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
    public void handleGet(RoutingContext event) {
        //TODO: move pure get requests processing here
        HttpServerRequest request = event.request();
        String path = request.path();
        endResponse(event, OK, "GET method is not supported yet. Path is: " + path);
    }

    @Override
    public void handleOptions(RoutingContext event) {
        endResponse(event, OK, "OPTION method is not supported yet");
    }

}
