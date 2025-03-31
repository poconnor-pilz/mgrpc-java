package io.mgrpc;

import io.mgrpc.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class providing server and channel conduits that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessConduit {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<String, ChannelListener> channelsById = new ConcurrentHashMap<>();
    private ServerListener server;

    private final ServerConduit serverConduit = new InprocServerConduit();

    private final Map<String, String> callIdToChannelIdMap = new ConcurrentHashMap<>();

    private static volatile Executor executorSingleton;

    private final InprocChannelConduit channelConduit = new InprocChannelConduit();


    public ChannelConduitManager getChannelConduitManager(){
        return new ChannelConduitManager() {
            @Override
            public ChannelConduit getChannelConduitForServer(String serverTopic, ChannelListener channelListener) {
                channelConduit.start(channelListener);
                return channelConduit;
            }
            @Override
            public Executor getExecutor() {
                return getExecutorInstance();
            }
            @Override
            public void close(String channelId, String channelStatusTopic) {}
        };
    }

    public ServerConduit getServerConduit(){
        return serverConduit;
    }

    private static Executor getExecutorInstance() {
        if (executorSingleton == null) {
            synchronized (MessageServer.class) {
                if (executorSingleton == null) {
                    //Note that the default exector for grpc classic is a cached thread pool.
                    //The cached thread pool will retire threads that are not used for 60 seconds but otherwise
                    //create, cache and re-use threads as needed.
                    executorSingleton = Executors.newCachedThreadPool(new ThreadFactory() {
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "mgrpc-inproc-channel-" + threadNumber.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return executorSingleton;
    }

    private class InprocServerConduit implements ServerConduit {


        @Override
        public void start(ServerListener server) throws MessagingException {
            if(InProcessConduit.this.server != null){
                String err = "InProcessConduit instance can only be associated with one Server";
                log.error(err);
                throw new MessagingException(err);
            }
            InProcessConduit.this.server = server;
        }

        @Override
        public void close() {}

        @Override
        public void onCallClosed(String callId) {
            callIdToChannelIdMap.remove(callId);
        }

        @Override
        public void request(String callId, int numMessages) {
        }

        @Override
        public void send(RpcMessage message) throws MessagingException {
            final String channelId = callIdToChannelIdMap.get(message.getCallId());
            if(channelId == null){
                String err = "Channel for call " + message.getCallId() +  " does not exist";
                log.error(err);
                throw new MessagingException(err);
            }

            final ChannelListener channel = channelsById.get(channelId);
            if(channel == null){
                String err = "Channel " + channelId +  " does not exist";
                log.error(err);
                throw new MessagingException(err);
            }
            channel.onMessage(message);
        }

        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }

    }

    private class InprocChannelConduit implements ChannelConduit {

        private String channelId;

        @Override
        public void start(ChannelListener channel) {
            if(this.channelId == null) {
                this.channelId = channel.getChannelId();
                channelsById.put(channelId, channel);
            }
        }

        @Override
        public void onCallClosed(String callId){}

        @Override
        public void request(String callId, int numMessages) {}

        @Override
        public void close() {
            channelsById.remove(channelId);
        }

        @Override
        public void send(RpcMessage.Builder rpcMessageBuilder) throws MessagingException {
            if(rpcMessageBuilder.hasStart()){
                callIdToChannelIdMap.put(rpcMessageBuilder.getCallId(), rpcMessageBuilder.getStart().getChannelId());
            }
            server.onMessage(rpcMessageBuilder.build());
        }

    }

}
