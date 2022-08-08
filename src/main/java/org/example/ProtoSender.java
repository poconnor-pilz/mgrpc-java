package org.example;

import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;
import com.pilz.mqttwrap.MqttPType;
import com.pilz.mqttwrap.MqttProtoReply;
import com.pilz.mqttwrap.MqttProtoRequest;
import com.pilz.mqttwrap.MqttProtoRequest.Builder;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ProtoSender {


    //TODO: Decrease this timeout
    public static final long REPLY_TIMEOUT_MILLIS = 100 * 1000;
    public static final long SUBSCRIPTION_TIMEOUT_MILLIS = 3 * 1000;

    private final MqttAsyncClient client;
    private final String serviceBaseTopic;

    public ProtoSender(MqttAsyncClient client, String serviceBaseTopic) {
        this.client = client;
        this.serviceBaseTopic = serviceBaseTopic;
    }

    public ByteString blockingSend(String method, MessageLite params) throws Exception{
        return blockingSend(method, params, REPLY_TIMEOUT_MILLIS);
    }

    public ByteString blockingSend(String method, MessageLite params, long timeoutMillis) throws Exception{

        CountDownLatch latch = new CountDownLatch(1);
        final ByteString[] reply = new ByteString[1];
        sendSingleRequest( method, params, new MqttProtoBufferObserver() {
            @Override
            public void onNext(ByteString value) {
                latch.countDown();
            }

            @Override
            public void onLast(ByteString value) {
                reply[0] = value;
                latch.countDown();
            }
            @Override
            public void onError(String error) {
                //TODO: Handle error
                System.out.println("********Error******** " + error);
            }

        });

        if(!latch.await(timeoutMillis, TimeUnit.MILLISECONDS)){
            throw new Exception("Failed to receive reply after " + timeoutMillis + " milliseconds");
        }

        if(reply[0] == null){
            throw new Exception("blockingSend received an onNext but it should only be used with service that sends" +
                    "a single onLast().");
        }


        return reply[0];
    }


    /**
     * Send a request to a service method that takes an input stream
     * @param method The name of the method to call
     * @param streamId The stream id if the server method takes an input stream from the client otherwise null
     * @param protoListener Listener for responses if this send is a request. Null if this send is part of an input
     *                      stream
     * @throws Exception
     */
    public void sendInputStreamRequest(String method, String streamId, MqttProtoBufferObserver protoListener) throws Exception{
        String replyTo = subscribeForReplies(method, protoListener);
        Builder request = MqttProtoRequest.newBuilder()
                .setStreamId(streamId)
                .setReplyTo(replyTo);
        send(method, request);
    }

    /**
     * Send a request to a service method
     * @param method The name of the method to call
     * @param message The payload of the message
     * @param protoListener Listener for responses if this send is a request. Null if this send is part of an input
     *                      stream
     * @throws Exception
     */
    public void sendSingleRequest(String method, MessageLite message, MqttProtoBufferObserver protoListener) throws Exception{
        String replyTo = subscribeForReplies(method, protoListener);
        Builder request = MqttProtoRequest.newBuilder()
                .setType(MqttPType.LAST)
                .setMessage(message.toByteString())
                .setReplyTo(replyTo);
        send(method, request);
    }

    /**
     * Send an error to a method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param error The error
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendErrorToStream(String method, String streamId, String error) throws Exception{
        Builder request = MqttProtoRequest.newBuilder()
                .setType(MqttPType.ERROR)
                .setError(error)
                .setStreamId(streamId);
        send(method, request);
    }

    /**
     * Send the last stream value to a method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param message The payload of the message
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendLastStreamValue(String method, String streamId, MessageLite message) throws Exception{
        Builder request = MqttProtoRequest.newBuilder()
                .setType(MqttPType.LAST)
                .setMessage(message.toByteString())
                .setStreamId(streamId);
        send(method, request);
    }


    /**
     * Send a stream value to a method that takes a input stream (client side stream)
     * @param method The name of the method to call
     * @param message The payload of the message
     * @param streamId The stream id if the service method takes an input stream from the client otherwise null
     * @throws Exception
     */
    public void sendNextStreamValue(String method, String streamId, MessageLite message) throws Exception{
        Builder request = MqttProtoRequest.newBuilder()
                .setType(MqttPType.NEXT)
                .setMessage(message.toByteString())
                .setStreamId(streamId);
        send(method, request);
    }

    private void send(String method, Builder request) throws Exception{
        final String topic = serviceBaseTopic + '/' + MqttProtoConsts.IN + '/' + method;
        client.publish(topic, new MqttMessage(request.build().toByteArray()));
    }


    private String subscribeForReplies(String method, MqttProtoBufferObserver protoListener) throws Exception{
        final String replyTo = serviceBaseTopic + '/' + MqttProtoConsts.OUT + '/' + method + '/' + Base64Utils.randomId();
        Logit.log("ProtoSender subscribing for reply on: " + replyTo);
        final IMqttMessageListener messageListener = new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                Logit.log("ProtoSender received reply on: " + topic);
                //TODO: catch parsing exception here
                MqttProtoReply mqttProtoReply = MqttProtoReply.parseFrom(message.getPayload());
                switch(mqttProtoReply.getType()){
                    case NEXT:
                        protoListener.onNext(mqttProtoReply.getMessage());
                        break;
                    case LAST:
                        Logit.log("ProtoSender Unsubscribing to: " + replyTo);
                        client.unsubscribe(replyTo);
                        protoListener.onLast(mqttProtoReply.getMessage());
                        break;
                    case ERROR:
                        Logit.log("ProtoSender error recieved Unsubscribing to: " + replyTo);
                        client.unsubscribe(replyTo);
                        protoListener.onError(mqttProtoReply.getError());
                        break;
                    default:
                        Logit.error("Unhandled message type");
                }
            }
        };
        client.subscribe(replyTo, 1, messageListener).waitForCompletion(SUBSCRIPTION_TIMEOUT_MILLIS); //TODO: check for timeout
        return replyTo;
    }

}
