package io.mgrpc.messaging;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface ServerMessageTransport {


    void start(ServerMessageListener listener) throws MessagingException;

    void close();

    /**
     * Send a message reply to a channel.
     * @param channelId The id of the client.
     * @param methodName The full method name of the gRPC service method e.g.
     *                   helloworld.ExampleHelloService/SayHello
     * @param buffer The payload of the message to send
     * @exception
     */
    void send(String channelId, String methodName, byte[] buffer) throws MessagingException;


}
