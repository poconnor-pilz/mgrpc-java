package io.mgrpc;

import java.util.concurrent.Executor;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface ServerConduit {


    /**
     * Called by the server when the server starts
     * @param server The server
     * @throws MessagingException
     */
    void start(ServerListener server) throws MessagingException;

    /**
     * Called by the server when the server closes. The conduit should release any resources here.
     */
    void close();

    /**
     * Called by the server after the call has closed.
     * The conduit can clean up any resources here for the call.
     */
    void onCallClosed(String callId);

    /**
     * Request the conduit to send on a number of messages for a call
     * If the conduit does not implement buffering it can ignore this and just send the messages
     * whenever they arrive.
     */
    void request(String callId, int numMessages);

    /**
     * Send a reply message to a channel.
     */
    void send(RpcMessage message) throws MessagingException;

    /**
     * @return The amount of credit that should be issued for flow control e.g. if flow credit is 20
     * then the sender will only send 20 messages before waiting for the receiver to send more flow credit.
     */
    int getFlowCredit();

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();
}
