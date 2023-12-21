package io.mgrpc.messaging.pubsub;


import com.google.protobuf.Parser;
import io.grpc.stub.StreamObserver;
import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerMessageTransport;

/**
 * Some channel {@link ServerMessageTransport}s may implement this interface if they want to support pub/sub
 * of gRPC streams. It is not necessary to implement this to support core gRPC
 */
public interface MessageSubscriber {

    /**
     * Subscribe for responses from a service. To use this the client should specify a RESPONSE_TOPIC when constructing
     * a stub for the service, for example:
     * MyService.newBlockingStub(channel).withOption(MsgChannel.RESPONSE_TOPIC, "mydevice/o/myresponsetopic");
     * Before issuing the call the client should first subscribe for responses e.g.
     * channel.subscribe("mydevice/o/myresponsetopic", HelloReply.parser(), myStreamObserver);
     * The subscription will automatically be closed when the response stream is completed but the client
     * can unsubscribe at any time using channel.unsubscribe("mydevice/o/myresponsetopic");
     *
     * @param responseTopic  The topic to which to send responses. All responses will be sent to this topic and the
     *                       stub will not receive any direct responses.
     * @param parser         The parser corresponding to the response type e.g. HelloReply.parser()
     * @param streamObserver Each observer that it subscribed to a responseTopic will receive all responses.
     * @param <T>            The type of the response e.g. HelloReply
     */
    <T> void subscribe(String responseTopic, Parser<T> parser, final StreamObserver<T> streamObserver) throws MessagingException;

    /**
     * Unsubscribe all StreamObservers from responseTopic
     */
    void unsubscribe(String responseTopic) throws MessagingException;

    /**
     * Unsubscribe a specific StreamObserver from responseTopic
     */
    void unsubscribe(String responseTopic, StreamObserver observer) throws MessagingException;
}
