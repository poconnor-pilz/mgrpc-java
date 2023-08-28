package io.mgrpc.examples.hello;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.mgrpc.*;
import io.mgrpc.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.*;

public class TestProxy {

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


        //HttpChannel -> HttpServer -> GrpcProxy -> MqttChannel -> Broker -> MqttServer
        int port = 50051;
        String target = "localhost:" + port;
        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shrt(Id.random());

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();

        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient(null);
        final String clientId = Id.random();
        MqttChannel mqttChannel = new MqttChannel(clientMqttConnection, clientId, SERVER);
        mqttChannel.init();

        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(mqttChannel);

        Server httpServer = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build().start();

        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new ServerAuthInterceptor());

        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient(null);
        MqttServer mqttServer = new MqttServer(serverMqttConnection, SERVER);
        mqttServer.addService(serviceWithIntercept);
        mqttServer.init();

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
                .newBlockingStub(httpChannel).withCallCredentials(token);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals(testclientId + level, helloReply.getMessage());

        //Verify that not setting authentication causes failure
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 =
                ExampleHelloServiceGrpc.newBlockingStub(httpChannel);
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> stub1.sayHello(joe));
        assertEquals(Status.UNAUTHENTICATED.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Authorization token is missing"));


        httpChannel.shutdown();
        httpServer.shutdown();
        mqttChannel.close();
        mqttServer.close();
    }


    @Test
    public void testServerSideProxy() throws Exception{


        //MqttChannel -> Broker -> MqttServer ->  GrpcProxy -> HttpChannel -> HttpServer
        int port = 50051;
        String target = "localhost:" + port;
        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shrt(Id.random());

        final MqttAsyncClient clientMqttConnection = MqttUtils.makeClient(null);
        final String clientId = Id.random();
        MqttChannel mqttChannel = new MqttChannel(clientMqttConnection, clientId, SERVER);
        mqttChannel.init();


        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new ServerAuthInterceptor());


        Server httpServer = ServerBuilder.forPort(port)
                .addService(serviceWithIntercept)
                .build().start();

        ManagedChannel httpChannel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext().build();
        final GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(httpChannel);

        final MqttAsyncClient serverMqttConnection = MqttUtils.makeClient(null);
        MqttServer mqttServer = new MqttServer(serverMqttConnection, SERVER);
        mqttServer.setFallBackRegistry(new GrpcProxy.Registry(proxy));
        mqttServer.init();

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
                .newBlockingStub(mqttChannel).withCallCredentials(token);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final HelloReply helloReply = stub.sayHello(joe);

        assertEquals(testclientId + level, helloReply.getMessage());


        //Verify that not setting authentication causes failure
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 =
                ExampleHelloServiceGrpc.newBlockingStub(mqttChannel);
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> stub1.sayHello(joe));
        assertEquals(Status.UNAUTHENTICATED.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Authorization token is missing"));

        httpChannel.shutdown();
        httpServer.shutdown();
        mqttChannel.close();
        mqttServer.close();
    }


}
