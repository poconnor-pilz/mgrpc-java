package io.mgrpc;

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

    private final Map<String, InprocServerConduit> serverConduits = new ConcurrentHashMap<>();
    private final Map<String, InprocTopicConduit> topicConduits = new ConcurrentHashMap<>();

    private final Map<String, String> callIdToChannelIdMap = new ConcurrentHashMap<>();

    private static final int TWO_BILLION = 2*1000*1000*1000;


    private static volatile Executor executorSingleton;



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

    private static final InProcessConduit instance = new InProcessConduit();

    private InProcessConduit() {}

    public static InProcessConduit getInstance() {
        return instance;
    }

    private final ChannelConduit channelConduit = new ChannelConduit() {
        @Override
        public TopicConduit getTopicConduit(String serverTopic, ChannelListener channelListener) {
            InprocTopicConduit topicConduit = topicConduits.get(serverTopic);
            if(topicConduit == null) {
                topicConduit = new InprocTopicConduit(serverTopic);
                topicConduits.put(serverTopic, topicConduit);
                topicConduit.start(channelListener);
            }
            return topicConduit;
        }
        @Override
        public Executor getExecutor() {
            return getExecutorInstance();
        }
        @Override
        public void close(String channelId) {}
    };


    public ChannelConduit getChannelConduit(){
      return this.channelConduit;
    }

    public ServerConduit getServerConduit(String serverTopic){
        InprocServerConduit serverConduit = serverConduits.get(serverTopic);
        if(serverConduit == null) {
            serverConduit = new InprocServerConduit();
            serverConduits.put(serverTopic, serverConduit);
        }
        return serverConduit;
    }


    private class InprocServerConduit implements ServerConduit {

        public ServerListener server;

        @Override
        public void start(ServerListener server) throws MessagingException {
            this.server = server;
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

    private class InprocTopicConduit implements TopicConduit {

        private String channelId;

        private final String serverTopic;

        private InprocTopicConduit(String serverTopic) {
            this.serverTopic = serverTopic;
        }

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

            final InprocServerConduit inprocServerConduit = serverConduits.get(this.serverTopic);
            if(inprocServerConduit == null) {
                throw new RuntimeException("Server conduit " + this.serverTopic + " does not exist");
            }
            if(inprocServerConduit.server == null) {
                throw new RuntimeException("Server not set for topic " + this.serverTopic);
            }
            inprocServerConduit.server.onMessage(rpcMessageBuilder.build());
        }

    }

}
