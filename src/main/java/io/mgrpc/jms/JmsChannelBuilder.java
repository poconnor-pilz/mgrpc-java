package io.mgrpc.jms;

import io.mgrpc.MessageChannel;

import javax.jms.Connection;

public class JmsChannelBuilder {

    private static final int DEFAULT_FLOW_CREDIT = 100;

    private Connection connection;


    private boolean useBrokerFlowControl;

    private String channelStatusTopic;
    private int flowCredit = DEFAULT_FLOW_CREDIT;


    private String channelId;

    private int queueSize;


    public JmsChannelBuilder setConnection(Connection client) {
        this.connection = client;
        return this;
    }

    public JmsChannelBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    public JmsChannelBuilder setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }
    public JmsChannelBuilder setUseBrokerFlowControl(boolean useBrokerFlowControl) {
        this.useBrokerFlowControl = useBrokerFlowControl;
        return this;
    }


    public JmsChannelBuilder setChannelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    public JmsChannelBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageChannel build(){
        final JmsChannelConduit conduit = new JmsChannelConduit(connection, useBrokerFlowControl, channelStatusTopic);
        return new MessageChannel(conduit, channelId, queueSize, flowCredit);
    }
}
