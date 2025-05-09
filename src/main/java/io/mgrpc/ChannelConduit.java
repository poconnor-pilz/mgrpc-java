package io.mgrpc;

import java.util.concurrent.Executor;

public interface ChannelConduit {

    /**
     * There is a channel topic conduit per server. This method will return the channel topic conduit corresponding
     * to the server. If the conduit is not started it will start it which may cause it to block for some time.
     * @param serverTopic The root topic of the server corresponding to the conduit
     * @return
     */
    TopicConduit getTopicConduit(String serverTopic, ChannelListener channelListener);

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();

    /**
     * @param channelId The id of the channel being closed
     */
    void close(String channelId);
}
