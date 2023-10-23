package io.mgrpc.messaging;

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
     * Called by the channel when the channel closes. The transport should release any resources here.
     */
    void close();

    /**
     * Send a request to a server.
     * @param methodName The full method name of the gRPC service method e.g.
     *                   helloworld.ExampleHelloService/SayHello
     * @param buffer The payload of the message to send
     * @exception
     */
    void send(String methodName, byte[] buffer) throws MessagingException;


}
