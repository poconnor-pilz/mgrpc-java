package io.mgrpc.mqtt;

import io.mgrpc.TimerService;
import io.mgrpc.TopicConduit;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class used to
 */
public class MqttTopicConduitManager {

    public static int TOPIC_CONDUITS_PER_CLIENT = 20;

    public static int GC_INTERVAL_MS = 5*60*1000;


    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    /**
     * We need to limit the users associated with an mqtt client
     * Each TopicConduit corresponds to two subscriptions and subscriptions per client are limited on AWS
     * (the default is 50 but it may be adjustable)
     * Also the paho client is limited in number of parallel messages so this would help deal with that also
     * although it would be better to scale for that based on concurrent grpc calls)
     * This class helps maintain a count of the users (TopicConduits) of the client
     */
    private static class LimitedClient {
        final IMqttAsyncClient mqttClient;

        /**
         * The count of TopicConduits using the  limited client.
         */
        int topicConduitCount;

        private LimitedClient(IMqttAsyncClient mqttClient) {
            this.mqttClient = mqttClient;
            this.topicConduitCount = 0;
        }
    }

    List<LimitedClient> limitedClients = new ArrayList<>();

    Map<String, LimitedClient> limitedClientsByServerTopic = new ConcurrentHashMap<>();

    final Map<String, MqttTopicConduit> conduitsByServerTopic = new ConcurrentHashMap<>();

    final ScheduledFuture gcTask;

    private final MqttClientFactory clientFactory;

    public MqttTopicConduitManager(MqttClientFactory clientFactory) {

        this.clientFactory = clientFactory;

        /*
        There is a limit on the number of subscriptions per connection (and ultimately also per account).
        Each TopicConduit needs to keep a couple of subscriptions alive to listen for server status and server replies.
        If no client is using the TopicConduit (or the server) then we want to drop the subscriptions.
        So we create a task here that will run every GC_INTERVAL_MS milliseconds. It checks to see if
        any of the TopicConduit's have no ongoing calls and have not had any messages sent for the last
        GC_INTERVAL_MS milliseconds. If so the TopicConduit will be removed and its subscriptions will be dropped.
        In the worst case an idle TopicConduit would not be shut down for (GC_INTERVAL_MS * 2) milliseconds
         */

        log.debug("Creating gc task with interval {} ms", GC_INTERVAL_MS );
        gcTask = TimerService.get().scheduleAtFixedRate(() -> {
            List<MqttTopicConduit> conduitsToStop = new ArrayList<>();
            //Synchronize this code so that there is no chance that the code in getTopicConduit can
            //execute at the same time.
            synchronized (this) {
                for(String serverTopic : conduitsByServerTopic.keySet()) {
                    final MqttTopicConduit topicConduit = conduitsByServerTopic.get(serverTopic);
                    final long idleTime = System.currentTimeMillis() - topicConduit.getTimeLastUsed();
                    log.debug("Idle time for serverTopic {}: {} ms", serverTopic, idleTime);
                    final long maxIdleTime = GC_INTERVAL_MS;
                    if(topicConduit.getNumOpenCalls() == 0 && idleTime > maxIdleTime) {
                        log.debug("Removing TopicConduit for topic {} because it hasn't been used for {} ms", serverTopic, idleTime);
                        conduitsByServerTopic.remove(serverTopic);
                        limitedClientsByServerTopic.get(serverTopic).topicConduitCount--;
                        limitedClientsByServerTopic.remove(serverTopic);
                        conduitsToStop.add(topicConduit);
                    }
                }
            }
            //We can do the unsubscribe outside the synchnronized block because for paho any new subscription
            //will override the old subscription anyway. i.e. if a new call happens to come in for the same topic
            //that we are garbage collecting here then there will be a new TopicConduit created for it that will
            //create a new subscription that will override the old one. So the old TopicConduit will not receive
            //messages on its old subscription.
            for(MqttTopicConduit topicConduit : conduitsToStop) {
                topicConduit.unsubscribe();
            }
        }, GC_INTERVAL_MS, GC_INTERVAL_MS, TimeUnit.MILLISECONDS);

    }

    /**
     * @return Main client for use by channel conduit for non topic specific work.
     * This main client may also be used by topic conduits (up to its limit)
     * This method should only be called once by channel conduit in its constructor.
     */
    public IMqttAsyncClient makeMainClient() {
        LimitedClient lc = new LimitedClient(clientFactory.createMqttClient());
        lc.topicConduitCount++;
        limitedClients.add(lc);
        return lc.mqttClient;
    }

   public synchronized TopicConduit getTopicConduit(String serverTopic, int flowCredit) {

        MqttTopicConduit conduit;
        conduit = conduitsByServerTopic.get(serverTopic);
        if (conduit == null) {
            LimitedClient lc = null;
            for (LimitedClient limitedClient : limitedClients) {
                if (limitedClient.topicConduitCount <= TOPIC_CONDUITS_PER_CLIENT) {
                    lc = limitedClient;
                    break;
                }
            }
            if (lc == null) {
                lc = new LimitedClient(clientFactory.createMqttClient());
                limitedClients.add(lc);
            }
            lc.topicConduitCount++;
            limitedClientsByServerTopic.put(serverTopic, lc);
            conduit = new MqttTopicConduit(lc.mqttClient, serverTopic, flowCredit);
            conduitsByServerTopic.put(serverTopic, conduit);
        }
        return conduit;
    }


}
