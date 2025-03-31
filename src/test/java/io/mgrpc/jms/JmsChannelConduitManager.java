package io.mgrpc.jms;

import io.mgrpc.ChannelConduitManager;
import io.mgrpc.ConnectionStatus;
import io.mgrpc.MessageServer;
import io.mgrpc.messaging.ChannelConduit;
import io.mgrpc.messaging.ChannelListener;
import io.mgrpc.messaging.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class JmsChannelConduitManager implements ChannelConduitManager {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private final Connection client;
    private final boolean useBrokerFlowControl;


    private static volatile Executor executorSingleton;

    private final Map<String, ChannelConduit> conduitsByServerTopic = new ConcurrentHashMap<>();



    private Session session;


    @Override
    public Executor getExecutor() {
        return getExecutorInstance();
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
                            Thread t = new Thread(r, "mgrpc-jms-channel-" + threadNumber.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return executorSingleton;
    }


    public JmsChannelConduitManager(Connection client, boolean useBrokerFlowControl) {
        this.client = client;
        this.useBrokerFlowControl = useBrokerFlowControl;
    }

    @Override
    public ChannelConduit getChannelConduitForServer(String serverTopic, ChannelListener channelListener) {

        if(session == null) {
            try {
                session = client.createSession();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }

        ChannelConduit conduit;
        synchronized (conduitsByServerTopic) {
            conduit = conduitsByServerTopic.get(serverTopic);
            if (conduit == null) {
                conduit = new JmsChannelConduit(session, serverTopic, useBrokerFlowControl, getExecutor());
                conduitsByServerTopic.put(serverTopic, conduit);
            }
        }
        try {
            //start should be idempotent and synchronized
            conduit.start(channelListener);
            return conduit;
        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void close(String channelId, String channelStatusTopic) {

        for (ChannelConduit conduit : conduitsByServerTopic.values()) {
            conduit.close();
        }

        try {
            //Notify that this client has been closed so that any server with ongoing calls can cancel them and
            //release resources.
            if(channelStatusTopic != null){
                String statusQueue = channelStatusTopic;
                log.debug("Closing channel. Sending notification on " + statusQueue);
                final byte[] connectedMsg = ConnectionStatus.newBuilder()
                        .setConnected(false)
                        .setChannelId(channelId)
                        .build().toByteArray();
                Queue queue = session.createQueue(statusQueue);
                MessageProducer producer = session.createProducer(queue);
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(connectedMsg);
                producer.send(bytesMessage);
            }

        } catch (JMSException e) {
            log.error("Exception sending channel close notification " + e);
        }


        //Close the session. This will close all other resources (publishers etc) for all conduits
        try {
            if(session != null){
                session.close();
            }
        } catch (JMSException e) {
            log.error("Exception closing jms session " + e);
        }
    }
}
