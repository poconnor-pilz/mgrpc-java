package io.mgrpc.examples.hello;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.*;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestScalableProxyServer {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {

        private final String response;

        public ListenForHello(String response){
            this.response = response;
        }
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            final HelloReply reply = HelloReply.newBuilder().setMessage(this.response).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    @BeforeAll
    public static void startEmbeddedBroker() throws Exception {
        EmbeddedBroker.start();
    }


    @Test
    public void testClientSideProxy() throws Exception{

        //Set the gc interval to one second so that we can test if topics got garbage collected
        MqttTopicConduitManager.GC_INTERVAL_MS = 500;

        //Set the limit to 2 to force use of a new mqtt client after two topics are used.
        MqttTopicConduitManager.TOPIC_CONDUITS_PER_CLIENT = 2;

        class ClientFactory implements MqttClientFactory{

            public List<MqttAsyncClient> clients = new ArrayList<>();
            @Override
            public IMqttAsyncClient createMqttClient() {
                try {
                    final MqttAsyncClient mqttAsyncClient = MqttUtils.makeClient();
                    clients.add(mqttAsyncClient);
                    return mqttAsyncClient;
                } catch (Exception e) {
                    log.error("Failed to create client", e);
                    return null;
                }
            }
        };

        final ClientFactory clientFactory = new ClientFactory();

        final MqttChannelConduit mqttChannelConduit = new MqttChannelConduit(clientFactory);

        MessageChannel messageChannel = new MessageChannel(mqttChannelConduit);


        //This tests the case where a http client calls a service in an MqttServer

        //HttpChannel -> HttpServer -> GrpcProxy -> MqttChannel -> Broker -> MqttServer
        int port = 50051;
        String target = "localhost:" + port;

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        //We want to wire this:
        //HttpServer -> GrpcProxy -> MqttChannel

        //Connect the proxy to the MessageChannel
        GrpcProxy proxy = new GrpcProxy(messageChannel);
        final GrpcProxy.Registry registry = new GrpcProxy.Registry(proxy);

        //Tell the proxy about the method types so that the message conduit can transport the messages
        //more efficiently (the test would still work without this but more mqtt messages would be transferred)
        registry.registerServiceDescriptor(ExampleHelloServiceGrpc.getServiceDescriptor());


        //Connect the proxy to the http server
        Server httpServer = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(registry)
                .build().start();

        int numServers = 6;
        List<String> serverNames = new ArrayList<>(numServers);
        for (int i = 0; i < numServers; i++) {
            serverNames.add("server" + i + "_" + Id.random());
        }

        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient();

        for(String serverName : serverNames){
            MessageServer messageServer = new MessageServer(new MqttServerConduit(serverMqttConnection, serverName));
            messageServer.addService(new ListenForHello(serverName));
            messageServer.start();
        }

        HelloRequest request = HelloRequest.newBuilder().setName("joe").build();

        ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub;
        HelloReply reply;

        stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, serverNames.get(0)));
        reply = stub.sayHello(request);
        assertEquals(serverNames.get(0), reply.getMessage());
        //There should only be one client because there is only one TopicConduit
        assertEquals(1, mqttChannelConduit.getStats().getNumClients());

        stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, serverNames.get(1)));
        reply = stub.sayHello(request);
        assertEquals(serverNames.get(1), reply.getMessage());
        //There should only be one client because there are only two TopicConduit's and
        //there can be up to 2 TopicConduit's per client
        assertEquals(1, mqttChannelConduit.getStats().getNumClients());

        //Sleep so that the first two TopicConduits get garbage collected
        Thread.sleep(1100);

        stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, serverNames.get(2)));
        reply = stub.sayHello(request);
        assertEquals(serverNames.get(2), reply.getMessage());
        //There should only be one client because there is only one TopicConduit
        assertEquals(1, mqttChannelConduit.getStats().getNumClients());

        stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, serverNames.get(3)));
        reply = stub.sayHello(request);
        assertEquals(serverNames.get(3), reply.getMessage());
        //There should only be one client because there are only two TopicConduit's and
        //there can be up to 2 TopicConduit's per client
        assertEquals(1, mqttChannelConduit.getStats().getNumClients());

        stub = ExampleHelloServiceGrpc.newBlockingStub(TopicInterceptor.intercept(httpChannel, serverNames.get(4)));
        reply = stub.sayHello(request);
        assertEquals(serverNames.get(4), reply.getMessage());
        //There should be two clients because there are 3 TopicConduits
        //and there can only be up to 2 TopicConduit's per client
        assertEquals(2, mqttChannelConduit.getStats().getNumClients());

        httpChannel.shutdown();
        httpServer.shutdown();

        serverMqttConnection.disconnect();
        serverMqttConnection.close();


        for(MqttAsyncClient client: clientFactory.clients){
            client.disconnect();
            client.close();
        }
    }

}
