package io.mgrpc.examples.hello;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttServerConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.*;

public class TestGrpcProxy {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            String clientId = ServerAuthInterceptor.CLIENT_ID_CONTEXT_KEY.get();
            Integer level = ServerAuthInterceptor.LEVEL_CONTEXT_KEY.get();
            final HelloReply reply = HelloReply.newBuilder().setMessage(clientId + level).build();
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

        //This tests the case where a http client calls a service in an MqttServer

        //HttpChannel -> HttpServer -> GrpcProxy -> MqttChannel -> Broker -> MqttServer
        int port = 50051;
        String target = "localhost:" + port;
        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shortRandom();

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final Channel httpChannelWithTopic = TopicInterceptor.intercept(httpChannel, SERVER);

        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient();
        MessageChannel messageChannel = new MessageChannel(new MqttChannelConduit(clientMqttConnection));

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


        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new ServerAuthInterceptor());


        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient();
        MessageServer messageServer = new MessageServer(new MqttServerConduit(serverMqttConnection, SERVER));
        messageServer.addService(serviceWithIntercept);
        messageServer.start();

        //Make a jwt token and add it to the call credentials
        final String testclientId = "aTestClientID";
        final Integer level = Integer.valueOf(9);
        final String jwtString = Jwts.builder()
                .setSubject(testclientId) // client's identifier
                .claim(ServerAuthInterceptor.LEVEL, Integer.valueOf(9))
                .signWith(SignatureAlgorithm.HS256, ServerAuthInterceptor.JWT_SIGNING_KEY)
                .compact();
        BearerToken token = new BearerToken(jwtString);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(httpChannelWithTopic).withCallCredentials(token);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals(testclientId + level, helloReply.getMessage());

        //Verify that not setting authentication causes failure
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 =
                ExampleHelloServiceGrpc.newBlockingStub(httpChannelWithTopic);
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> stub1.sayHello(joe));
        assertEquals(Status.UNAUTHENTICATED.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Authorization token is missing"));


        httpChannel.shutdown();
        httpServer.shutdown();
        messageChannel.close();
        messageServer.close();
    }


    @Test
    public void testServerSideProxy() throws Exception{

        //This tests the case where a java client that can connect to an mqtt broker can call
        //a http server on the server side of the broker that is mapped to a particular topic.
        //A jvm has to be on the server side of the broker that makes that connection.

        //MqttChannel -> Broker -> MqttServer ->  GrpcProxy -> HttpChannel -> HttpServer
        int port = 50051;
        String target = "localhost:" + port;
        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shortRandom();

        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient();
        MessageChannel messageChannel = new MessageChannel(new MqttChannelConduit(clientMqttConnection));

        final Channel messageChannelWithTopic = TopicInterceptor.intercept(messageChannel, SERVER);

        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new ServerAuthInterceptor());

        Server httpServer = ServerBuilder.forPort(port)
                .addService(serviceWithIntercept)
                .build().start();

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient();
        MessageServer messageServer = new MessageServer(new MqttServerConduit(serverMqttConnection, SERVER));

        //We want to wire this:
        //MqttServer ->  GrpcProxy -> HttpChannel

        //Connect the proxy to the http channel
        final GrpcProxy proxy2 = new GrpcProxy(httpChannel);
        //Connect the MessageServer to the proxy
        messageServer.setFallBackRegistry(new GrpcProxy.Registry(proxy2));

        messageServer.start();

        //Make a jwt token and add it to the call credentials
        final String testclientId = "aTestClientID";
        final Integer level = Integer.valueOf(9);
        final String jwtString = Jwts.builder()
                .setSubject(testclientId) // client's identifier
                .claim(ServerAuthInterceptor.LEVEL, Integer.valueOf(9))
                .signWith(SignatureAlgorithm.HS256, ServerAuthInterceptor.JWT_SIGNING_KEY)
                .compact();
        BearerToken token = new BearerToken(jwtString);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(messageChannelWithTopic).withCallCredentials(token);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals(testclientId + level, helloReply.getMessage());


        //Verify that not setting authentication causes failure
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 =
                ExampleHelloServiceGrpc.newBlockingStub(messageChannelWithTopic);
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> stub1.sayHello(joe));
        assertEquals(Status.UNAUTHENTICATED.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Authorization token is missing"));

        httpChannel.shutdown();
        httpServer.shutdown();
        messageChannel.close();
        messageServer.close();
    }


}
