package com.pilz.examples.hello;

import com.pilz.mqttgrpc.MqttChannel;
import com.pilz.mqttgrpc.MqttServer;
import com.pilz.mqttgrpc.Topics;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

public class TestAuth {

    private static final Logger log = LoggerFactory.getLogger(TestAuth.class);



    @Test
    void testAuthInterceptor() throws Exception {

        //Use AuthInterceptor to verify that the user is authorized test that it populates the context
        //with clientId and level
        class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                //Get the clientId and the level from the context. These will have been populated by the AuthInterceptor
                String clientId = AuthInterceptor.CLIENT_ID_CONTEXT_KEY.get();
                Integer level = AuthInterceptor.LEVEL_CONTEXT_KEY.get();
                final HelloReply reply = HelloReply.newBuilder().setMessage(clientId + level).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        }

        final String DEVICE = "device1";
        MqttServer server = new MqttServer(MqttUtils.makeClient(Topics.systemStatus(DEVICE)), DEVICE);
        server.init();

        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new AuthInterceptor());
        server.addService(serviceWithIntercept);
        MqttChannel channel = new MqttChannel(MqttUtils.makeClient(null), DEVICE);
        channel.init();


        //Make a jwt token and add it to the call credentials
        final String clientId = "aTestClientID";
        final Integer level = Integer.valueOf(9);
        final String jwtString = Jwts.builder()
                .setSubject(clientId) // client's identifier
                .claim(AuthInterceptor.LEVEL, Integer.valueOf(9))
                .signWith(SignatureAlgorithm.HS256, AuthInterceptor.JWT_SIGNING_KEY)
                .compact();
        BearerToken token = new BearerToken(jwtString);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub =
                ExampleHelloServiceGrpc.newBlockingStub(channel).withCallCredentials(token);
        final HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        HelloReply response = stub.sayHello(request);
        //HelloListener should have received values for clientId and level as part of the call context.
        assertEquals(clientId + level, response.getMessage());


        //Test without setting authentication
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub1 =
                ExampleHelloServiceGrpc.newBlockingStub(channel);
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> stub1.sayHello(request));
        assertEquals(Status.UNAUTHENTICATED.getCode(), ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("Authorization token is missing"));

        channel.close();
        server.close();

    }

}
