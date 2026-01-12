package io.mgrpc.jms;


import io.mgrpc.MessageServer;

import javax.jms.Connection;

public class JmsServerBuilder {


   private int creditSize = MessageServer.DEFAULT_CREDIT_SIZE;

    private String topic;

    private String channelStatusTopic;

    private int queueSize = MessageServer.DEFAULT_QUEUE_SIZE;

    private Connection connection;


    /**
     * @param connection The JMS connection for the server
     */
    public JmsServerBuilder setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * @param topic The root topic of the server e.g. "tenant1/device1"
     *              This topic should be unique to the broker.
     *              The server will subscribe for requests on subtopics of {serverTopic}/i/svc
     *              A request for a method should be sent to sent to {serverTopic}/i/svc/{slashedFullMethod}
     *              Replies will be sent to {serverTopic}/o/svc/{channelId}/{slashedFullMethod}
     *              Where if the gRPC fullMethodName is "helloworld.HelloService/SayHello"
     *              then {slashedFullMethod} is "helloworld/HelloService/SayHello"
     */
    public JmsServerBuilder setTopic(String topic) {
        this.topic = topic;
        return this;
    }


    /**
     * @param channelStatusTopic The topic on which messages regarding channel status will be reported.
     *                           If this value is null then the server will not attempt to subscribe for
     *                           channel status messages.
     */
    public JmsServerBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    /**
     * @param creditSize The amount of credit that should be issued for flow control e.g. if creditSize is 20
     *      then the sender will only send 20 messages before waiting for the receiver to send more credit.
     */
    public JmsServerBuilder setCreditSize(int creditSize) {
        this.creditSize = creditSize;
        return this;
    }


    /**
     * @param queueSize  The size of the internal in-memory message queue for each call
     */
    public JmsServerBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageServer build(){
        final JmsServerConduit conduit = new JmsServerConduit(connection, topic, channelStatusTopic);
        return new MessageServer(conduit, queueSize, creditSize);
    }
}
