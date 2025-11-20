package io.mgrpc;


import com.google.protobuf.Parser;
import io.grpc.stub.StreamObserver;

/**
 * Interface that does message broker publish subscribe in terms of gRPC streams.
 * For use cases where someone wants basic publish subscribe without gRPC services but still wants to work in terms
 * of streams and protocol buffers.
 * The streams will have guaranteed order and de-duplication.
 * This also supports multiple subscriptions to a server stream from a gRPC service. In that case the topic that the service
 * is publishing responses to needs to be known. mgRPC supports setting a header option to specify the topic
 * for example:
 * <pre>
 * {@code
 *  MyService.newBlockingStub(channel).withOption(MsgChannel.OPT_OUT_TOPIC, "mydevice/o/myresponsetopic");
 * }
 * </pre>
 */
public interface StreamPubSub {

    /**
     * Get a StreamObserver that will publish its messages to a broker topic
     * @param topic The topic to which the StreamObserver should publish messages
     * @return the StreamObserver
     * @param <T> The type of the messages that will be published.
     */
    public <T> StreamObserver<T> getPublisher(String topic);

   /**
     * Subscribe for a stream at a topic
     *
     * @param topic          The topic on which to subscribe for responses.
     * @param parser         The parser corresponding to the response type e.g. HelloReply.parser()
     * @param streamObserver Each observer that it subscribed to a topic will receive all responses.
     * @param <T>            The type of the response e.g. HelloReply
     */
    <T> void subscribe(String topic, Parser<T> parser, final StreamObserver<T> streamObserver) throws MessagingException;

    /**
     * Unsubscribe all StreamObservers from topic
     */
    void unsubscribe(String topic) throws MessagingException;

    /**
     * Unsubscribe a specific StreamObserver from topic
     */
    void unsubscribe(String topic, StreamObserver observer) throws MessagingException;

}