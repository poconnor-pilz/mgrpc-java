package io.mgrpc.jms;


import io.mgrpc.MessageServer;

import javax.jms.Connection;

public class JmsServerBuilder {




    private static final int TWO_BILLION = 2*1000*1000*1000;

    private int flowCredit = TWO_BILLION;


    private String topic;

    private String channelStatusTopic;

    private int queueSize = 100;

    private Connection connection;


    public JmsServerBuilder setConnection(Connection client) {
        this.connection = client;
        return this;
    }
    public JmsServerBuilder setTopic(String topic) {
        this.topic = topic;
        return this;
    }


    public JmsServerBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    public JmsServerBuilder setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }


    public JmsServerBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageServer build(){
        final JmsServerConduit conduit = new JmsServerConduit(connection, topic, channelStatusTopic, flowCredit);
        return new MessageServer(conduit, queueSize);
    }
}
