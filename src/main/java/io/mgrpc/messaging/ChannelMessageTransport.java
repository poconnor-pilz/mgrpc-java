package io.mgrpc.messaging;

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
    void start(ChannelMessageListener channel) throws MessagingException;


    /**
     * Called by the channel after the call has closed.
     * The transport can clean up any resources here for the call.
     */
    void onCallClosed(String callId);


    /**
     * Called by the channel when the channel closes. The transport should release any resources here.
     */
    void close();

    /**
     * Request the transport to send on a number of messages for a call
     * If the transport does not implement buffering it can ignore this and just send the messages
     * whenever they arrive.
     */
    void request(String callId, int numMessages);


    /**
     * Send a request to a server.
     * @exception
     */
    void send(RpcMessage.Builder rpcMessageBuilder) throws MessagingException;

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();


}
