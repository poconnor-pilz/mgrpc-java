package io.mgrpc.examples.hello;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelFactory;
import io.mgrpc.mqtt.MqttServerTransport;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class TestMessageProxy {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;



    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
    }

    @AfterAll
    public static void stopClients() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
    }


    @Test
    public void testTheProxy() throws Exception {


        String topic1 = "server1";
        String topic2 = "server2";

        MessageServer server1 = new MessageServer(new MqttServerTransport(serverMqtt, topic1));
        server1.start();
        server1.addService(new HelloServiceForTest());

        MessageServer server2 = new MessageServer(new MqttServerTransport(serverMqtt, topic2));
        server2.start();
        server2.addService(new HelloServiceForTest());

        final MqttChannelFactory mqttChannelFactory = new MqttChannelFactory(clientMqtt);

        MessageProxy proxy = new MessageProxy(mqttChannelFactory);

        int port = 8981;
        Server server = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(new MessageProxy.Registry(proxy))
                .build()
                .start();

        String target = "localhost:" + port;
        ManagedChannel rawChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        //Add the interceptor that will take the server topic and put it in the metadata header for the call
        final Channel channel = ClientInterceptors.intercept(rawChannel, new TopicInterceptor("server1"));



        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub
                = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = blockingStub.sayHello(joe);
        assertEquals("Hello joe", helloReply.getMessage());

    }

}
