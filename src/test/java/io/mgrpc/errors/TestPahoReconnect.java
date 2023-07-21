package io.mgrpc.errors;

import io.mgrpc.ServerTopics;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPahoReconnect {

    private static Logger log = LoggerFactory.getLogger(TestPahoReconnect.class);

    private static final String DEVICE = "device1";

    //@Test
    public void tryReconnect() throws Exception{


        //TODO: Got rid of embedded broker. Change this to use a socket that we can break.
        //MqttUtils.startEmbeddedBroker();

        final MqttAsyncClient client;
        client = new MqttAsyncClient(
                    "tcp://localhost:1884",
                    MqttAsyncClient.generateClientId(),
                    new MemoryPersistence());
        MqttConnectOptions options = new MqttConnectOptions();

        //final byte[] lwtMessage = ConnectionStatus.newBuilder().setConnected(false).build().toByteArray();
        //options.setWill(lwtTopic, lwtMessage, 1, true);
        options.setAutomaticReconnect(true);


        //We could do options.setCleanSession(false) here which will make a persistent session
        //For AWS the "Persistent session expiry period" is extendable up to 7 days.
        //https://docs.aws.amazon.com/general/latest/gr/iot-core.html#message-broker-limits
        //If that is reliable then we won't need any re-start notifications
        //as all subscriptions will be re-constituted. It will even deliver queued messages (but for grpc services
        //we probably would not want that. A stream should complete or not, not be in a half way state).
        //In that sense the clean session is easier. For example we don't want some sub to a random replyto to be
        //re-created. In fact the only sub we want re-created would be for something like a watch.
        //So watches might be done on a separate persistent session (clean=false)
        //But even with watches the main connection that will fail is the device/server and that is publishing
        //So it has to remember to publish anyway and persistent session will not help with that.
        //i.e. The only place the persistent session would be valuable is the cloud watch client which is unlikely to fail
        //So overall it's better to keep it 'dumb pipes' except for setting the re-connect automatically and notifiying
        //each grpc client and server when a connect or disconnect happens.

        final CountDownLatch latchCompleted = new CountDownLatch(2);
        final CountDownLatch latchLost = new CountDownLatch(1);

        //Use extended callback to detect connect and disconnect
        client.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.error("Connection completed to: " + serverURI);
                latchCompleted.countDown();
            }

            @Override
            public void connectionLost(Throwable cause) {
                log.error("Connection lost", cause);
                latchLost.countDown();
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {

            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }
        });


        client.connect(options).waitForCompletion();

        //MqttUtils.stopEmbeddedBroker();

        assertTrue(latchLost.await(5, TimeUnit.SECONDS));

        //MqttUtils.startEmbeddedBroker();

        assertTrue(latchCompleted.await(5, TimeUnit.SECONDS));
    }


}
