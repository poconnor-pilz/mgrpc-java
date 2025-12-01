package io.mgrpc;

/**
 * Interface to messaging client. Adapters should implement this to work with different message protocols.
 */
public interface TopicConduit {

    /**
     * Called by the channel when it starts. This should be idempotent.
     * @param channel The channel
     * @throws MessagingException
     */
    void start(ChannelListener channel) throws MessagingException;


    /**
     * Called by the channel after the call has closed.
     * The conduit can clean up any resources here for the call.
     */
    void onCallClosed(String callId);


    /**
     * Called by the channel when the channel closes. The conduit should release any resources here.
     */
    void close();

    /**
     * Request the conduit to send on a number of messages for a call
     * If the conduit does not implement buffering it can ignore this and just send the messages
     * whenever they arrive.
     */
    void request(String callId, int numMessages);


    /**
     * Send a request to a server.
     * @exception
     */
    void send(RpcMessage.Builder rpcMessageBuilder) throws MessagingException;

    /**
     * @return The amount of credit that should be issued for flow control e.g. if flow credit is 20
     * then the sender will only send 20 messages before waiting for the receiver to send more flow credit.
     */
    int getFlowCredit();

}
