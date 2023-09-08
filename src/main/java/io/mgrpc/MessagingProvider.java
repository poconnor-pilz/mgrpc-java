package io.mgrpc;

/**
 * Interface to messagin client. Adapters should implement this to work with different message protocols.
 */
public interface MessagingProvider {


    void connectListener(MessagingListener listener) throws MessagingException;

    void disconnectListener();

    /**
     * Send a message (request or reply) to a server or channel.
     * @param clientId The id of the client.
     * @param methodName The full method name of the gRPC service method e.g.
     *                   helloworld.ExampleHelloService/SayHello
     * @param buffer The payload of the message to send
     * @exception
     */
    void send(String clientId, String methodName, byte[] buffer) throws MessagingException;


}
