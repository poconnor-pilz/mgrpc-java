package io.mgrpc.examples.hello;

import io.grpc.*;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelTransport;
import io.mgrpc.mqtt.MqttServerTransport;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Make a http client and server where the http server proxies calls to the broker and test it
 * httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer
*/
public class TestHelloGrpcProxy extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

    MessageChannel messageChannel;
    MessageServer messageServer;
    Server httpServer;

    Channel httpChannel;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws Exception {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }


    @BeforeEach
    void setup() throws Exception{

        //We want to wire this:
        //httpChannel -> httpServer -> GrpcProxy -> messageChannel -> broker-> messageServer

        messageServer = new MessageServer(new MqttServerTransport(serverMqtt, SERVER));
        messageServer.start();
        messageServer.addService(new HelloServiceForTest());

        messageChannel = new MessageChannel(new MqttChannelTransport(clientMqtt, SERVER));
        messageChannel.start();

        GrpcProxy proxy = new GrpcProxy(messageChannel);

        int port1 = 8981;
        httpServer = Grpc.newServerBuilderForPort(port1, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build()
                .start();

        String target = "localhost:" + port1;
        httpChannel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();

    }

//    @BeforeEach
//    void setup() throws Exception{
//
//      //This setup is for http clients and servers.

//        //We want to wire this:
//        //channel -> server -> GrpcProxy -> channel2 -> server2
//
//        int port2 = 8982;
//        Server server2 = ServerBuilder.forPort(port2)
//                .addService(new HelloServiceForTest())
//                .build()
//                .start();
//
//        String target2 = "localhost:" + port2;
//        ManagedChannel channel2 = Grpc.newChannelBuilder(target2, InsecureChannelCredentials.create())
//                .build();
//
//        GrpcProxy2<byte[], byte[]> proxy = new GrpcProxy2<byte[], byte[]>(channel2);
//
//        int port1 = 8981;
//        Server server = Grpc.newServerBuilderForPort(port1, InsecureServerCredentials.create())
//                .fallbackHandlerRegistry(new GrpcProxy2.Registry(proxy))
//                .build()
//                .start();
//
//        String target = "localhost:" + port1;
//        channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
//                .build();
//
//    }

    @AfterEach
    void tearDown() throws Exception{
        messageChannel.close();
        messageServer.close();
        httpServer.shutdownNow();
        httpServer.awaitTermination();
    }


    @Override
    public Channel getChannel() {
        return httpChannel;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(messageServer.getStats().getActiveCalls(), numActiveCalls);
        assertEquals(messageChannel.getStats().getActiveCalls(), numActiveCalls);
    }
}
