package io.mgrpc.messaging;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface ChannelMessageTransport {


    void start(ChannelMessageListener listener) throws MessagingException;

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
