package io.mgrpc.jms;

import io.mgrpc.MessageChannel;

import javax.jms.Connection;

public class JmsChannelBuilder {

    private static final int DEFAULT_FLOW_CREDIT = 100;

    private Connection connection;


    private boolean useBrokerCallQueues;

    private String channelStatusTopic;
    private int flowCredit = DEFAULT_FLOW_CREDIT;


    private String channelId;

    private int queueSize;


    /**
     * @param connection The JMS connection for the channel
     */
    public JmsChannelBuilder setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     *@param channelStatusTopic The topic on which messages regarding the channel status will be published.
     *                           (For MQTT this topic will be the same topic as the MQTT LWT for the channel client)
     *                           If this value is null then the conduit will not attempt to publish
     *                           channel status messages.
     */
    public JmsChannelBuilder setChannelStatusTopic(String channelStatusTopic) {
        this.channelStatusTopic = channelStatusTopic;
        return this;
    }

    /**
     * @param flowCredit The amount of credit that should be issued for flow control e.g. if flow credit is 20
     *      then the sender will only send 20 messages before waiting for the receiver to send more flow credit.
     */
    public JmsChannelBuilder setFlowCredit(int flowCredit) {
        this.flowCredit = flowCredit;
        return this;
    }

    /**
     * @param useBrokerCallQueues If true then use broker queues to buffer client and server streams.
     */
    public JmsChannelBuilder setUseBrokerCallQueues(boolean useBrokerCallQueues) {
        this.useBrokerCallQueues = useBrokerCallQueues;
        return this;
    }


    /**
     * @param channelId The client id for the channel. Should be unique.
     */
    public JmsChannelBuilder setChannelId(String channelId) {
        this.channelId = channelId;
        return this;
    }

    /**
     * @param queueSize  The size of the internal in-memory message queue for each call's replies
     */
    public JmsChannelBuilder setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        return this;
    }

    public MessageChannel build(){
        final JmsChannelConduit conduit = new JmsChannelConduit(connection, useBrokerCallQueues, channelStatusTopic);
        return new MessageChannel(conduit, channelId, queueSize, flowCredit);
    }
}
