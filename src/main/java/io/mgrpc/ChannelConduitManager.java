package io.mgrpc;

import io.mgrpc.messaging.ChannelConduit;
import io.mgrpc.messaging.ChannelListener;

import java.util.concurrent.Executor;

public interface ChannelConduitManager {

    /**
     * There is a channel conduit per server. This method will return the channel conduit corresponding
     * to the server. If the conduit is not started it will start it which may cause it to block for some time.
     * @param serverTopic The root topic of the server corresponding to the conduit
     * @return
     */
    ChannelConduit getChannelConduitForServer(String serverTopic, ChannelListener channelListener);

    /**
     * @return The executor with which to execute calls
     */
    Executor getExecutor();

    void close(String channelId, String channelStatusTopic);
}
