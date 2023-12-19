package io.mgrpc.messaging;

import io.mgrpc.MessageServer;

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
    void start(MessageServer server) throws MessagingException;

    /**
     * Called by the server when the server closes. The transport should release any resources here.
     */
    void close();

    /**
     * Called by the server after the call has closed.
     * The transport can clean up any resources here for the call.
     */
    void onCallClose(String channelId, String callId);

    /**
     * Request the transport to send on a number of messages for a call
     * If the transport does not implement buffering it can ignore this and just send the messages
     * whenever they arrive.
     */
    void request(String channelId, String callId, int numMessages);

    /**
     * Send a message reply to a channel.
     * @param channelId The id of the channel.
     * @param callId The id of the call.
     * @param methodName The full method name of the gRPC service method e.g.
     *                   helloworld.ExampleHelloService/SayHello
     * @param buffer The payload of the message to send
     * @exception
     */
    void send(String channelId, String callId, String methodName, byte[] buffer) throws MessagingException;

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();
}
