package io.mgrpc.messaging;

import io.mgrpc.RpcMessage;

import java.util.concurrent.Executor;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface ServerMessageTransport {


    /**
     * Called by the server when the server starts
     * @param server The server
     * @throws MessagingException
     */
    void start(ServerMessageListener server) throws MessagingException;

    /**
     * Called by the server when the server closes. The transport should release any resources here.
     */
    void close();

    /**
     * Called by the server after the call has closed.
     * The transport can clean up any resources here for the call.
     */
    void onCallClosed(String callId);

    /**
     * Request the transport to send on a number of messages for a call
     * If the transport does not implement buffering it can ignore this and just send the messages
     * whenever they arrive.
     */
    void request(String callId, int numMessages);

    /**
     * Send a reply message to a channel.
     */
    void send(RpcMessage message) throws MessagingException;

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();
}
