package io.mgrpc.messaging;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.mgrpc.MessageChannel;
import io.mgrpc.RpcMessage;

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
     * Called by ClientCall.start. The transport can set up any resources it needs for the call at this point.
     * @param methodDescriptor
     * @param callOptions
     */
    void onCallStart(MethodDescriptor methodDescriptor, CallOptions callOptions, String callId);

    /**
     * Called by the channel after the call has closed.
     * The transport can clean up any resources here for the call.
     */
    void onCallClose(String callId);


    /**
     * Called by the channel when the channel closes. The transport should release any resources here.
     */
    void close();

    /**
     * Send a request to a server.
     * @exception
     */
    void send(MethodDescriptor methodDescriptor, RpcMessage.Builder rpcMessageBuilder) throws MessagingException;

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();


}
