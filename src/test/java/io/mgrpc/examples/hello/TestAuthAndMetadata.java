package io.mgrpc.examples.hello;

import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.Id;
import io.mgrpc.MqttChannel;
import io.mgrpc.MqttServer;
import io.mgrpc.utils.MqttUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.*;

public class TestAuthAndMetadata {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



    @Test
    void testAuthAndMetadata() throws Exception {

        EmbeddedBroker.start();

        //Use ServerAuthInterceptor to verify that the user is authorized test that it populates the context
        //with clientId and level
        //Also verify the that the HOSTNAME metadata value inserted by ClientMetadataInterceptor is correctly
        //merged with the Auth metadata

        class ListenForHello extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                //Get the clientId and the level from the context. These will have been populated by the AuthInterceptor
                String clientId = ServerAuthInterceptor.CLIENT_ID_CONTEXT_KEY.get();
                Integer level = ServerAuthInterceptor.LEVEL_CONTEXT_KEY.get();
                String hostName = ServerAuthInterceptor.HOSTNAME_CONTEXT_KEY.get();
                final HelloReply reply = HelloReply.newBuilder().setMessage(clientId + level + hostName).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        }

        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String SERVER = Id.shrt(Id.random());
        MqttServer server = new MqttServer(MqttUtils.makeClient(), SERVER);
        server.init();

        final ServerServiceDefinition serviceWithIntercept = ServerInterceptors.intercept(
                new ListenForHello(),
                new ServerAuthInterceptor());
        server.addService(serviceWithIntercept);
        final String clientId = Id.random();
        MqttChannel channel = new MqttChannel(MqttUtils.makeClient(null), clientId, SERVER);
        channel.init();


        //Make a jwt token and add it to the call credentials
        final String testClientId = "aTestClientID";
        final Integer level = Integer.valueOf(9);
        final String jwtString = Jwts.builder()
                .setSubject(testClientId) // client's identifier
                .claim(ServerAuthInterceptor.LEVEL, Integer.valueOf(9))
                .signWith(SignatureAlgorithm.HS256, ServerAuthInterceptor.JWT_SIGNING_KEY)
                .compact();
        BearerToken token = new BearerToken(jwtString);

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub =
                ExampleHelloServiceGrpc.newBlockingStub(channel)
                        .withCallCredentials(token)
                        .withInterceptors(new ClientMetadataInterceptor());
        final HelloRequest request = HelloRequest.newBuilder().setName("joe").build();
        HelloReply response = stub.sayHello(request);
        //HelloListener should have received values for clientId and level and hostName as part of the call context.
        assertEquals(testClientId + level + ClientMetadataInterceptor.MYHOST, response.getMessage());


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
