package io.mgrpc.messaging.pubsub;

import io.mgrpc.messaging.MessagingException;
import io.mgrpc.messaging.ServerMessageTransport;

/**
 * Some server {@link ServerMessageTransport}s may implement this interface if they want to support pub/sub
 * of gRPC streams. It is not necessary to implement this to support core gRPC
 */
public interface MessagePublisher {
    /**
     * Send a message (request or reply) to a server or channel.
     * @param topic The full broker topic on which to publish the buffer
     * @param buffer The payload of the message to send
     * @exception
     */
    void publish(String topic,  byte[] buffer) throws MessagingException;
}
