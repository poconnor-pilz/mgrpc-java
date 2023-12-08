package io.mgrpc.messaging;

import io.grpc.MethodDescriptor;
import io.mgrpc.MessageChannel;

import java.util.concurrent.Executor;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface ChannelMessageTransport {

    /**
     * Called by the channel when it starts
     * @param channel The channel
     * @throws MessagingException
     */
    void start(MessageChannel channel) throws MessagingException;

    /**
     * Called by the channel when the channel closes. The transport should release any resources here.
     */
    void close();

    /**
     * Send a request to a server.
     * @param isStart Is a start message.
     * @param callId The call id
     * @param methodDescriptor The descriptor of gRPC service method
     * @param buffer The payload of the message to send
     * @exception
     */
    void send(boolean isStart, String callId, MethodDescriptor methodDescriptor, byte[] buffer) throws MessagingException;

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();


}
