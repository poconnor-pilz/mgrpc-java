package io.mgrpc.errors;

import io.grpc.*;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.Id;
import io.mgrpc.MqttChannel;
import io.mgrpc.MqttServer;
import io.mgrpc.Topics;
import io.mgrpc.examples.hello.HelloServiceForTest;
import io.mgrpc.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCommsErrors {

    private static Logger log = LoggerFactory.getLogger(TestCommsErrors.class);

    private static final String DEVICE = "device1";

    private static final String SERVICE_NAME = "helloservice";

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    @BeforeAll
    public static void startClients() throws Exception {
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient(null);
    }


    class HelloService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
        @Override
        public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            while(true){
                responseObserver.onNext(HelloReply.newBuilder().setMessage("hi").build());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    @Test
    public void testServerNeverConnected() throws Exception{

        MqttChannel channel = new MqttChannel(clientMqtt, Id.random(), DEVICE);
        channel.init();
        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        assertTrue(errorObserver.errorLatch.await(5, TimeUnit.SECONDS));
        assertTrue(errorObserver.exception.getStatus().getCode().value() == Status.UNAVAILABLE.getCode().value());

    }

    @Test
    public void testServerConnectedButNotInitially() throws Exception{

        MqttChannel channel = new MqttChannel(clientMqtt, Id.random(), DEVICE);
        channel.init();
        ErrorObserver errorObserver = new ErrorObserver("obs");

        //Allow enough time for the initial ping to the server to fail
        Thread.sleep(500);

        //The server will send a connected status when it starts up
        MqttServer server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        server.addService(new HelloService());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        assertFalse(errorObserver.errorLatch.await(5, TimeUnit.SECONDS));

    }


    @Test
    public void testServerDisconnectedMidstream() throws Exception{

        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up
        MqttChannel channel = new MqttChannel(clientMqtt, Id.random(), DEVICE);
        channel.init();
        MqttAsyncClient serverMqttWithLwt = MqttUtils.makeClient(Topics.statusOut(DEVICE));
        MqttServer server = new MqttServer(serverMqttWithLwt, DEVICE);
        server.init();
        server.addService(new HelloService());

        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        Thread.sleep(500);
        server.close();
        assertTrue(errorObserver.errorLatch.await(5, TimeUnit.SECONDS));
        assertTrue(errorObserver.exception.getStatus().getCode().value() == Status.UNAVAILABLE.getCode().value());

    }


    @Test
    public void testServerLWTMidstream() throws Exception{

        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up
        MqttChannel channel = new MqttChannel(clientMqtt, Id.random(), DEVICE);
        channel.init();
        MqttServer server = new MqttServer(serverMqtt,  DEVICE);
        server.init();
        server.addService(new HelloService());

        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        Thread.sleep(500);
        //This should cause an LWT message which will send on the disconnected status
        serverMqtt.disconnectForcibly();
        assertTrue(errorObserver.errorLatch.await(5, TimeUnit.SECONDS));
        assertTrue(errorObserver.exception.getStatus().getCode().value() == Status.UNAVAILABLE.getCode().value());

    }

    
/*

    @Test
    public void testNoInitialClientBrokerConnection() throws MqttException {

        //Verify that if the client has no broker connection then it gets an UNAVAILABLE
        //error when it tries to make a call to a service


        //Setup the client stub
        //Make a MqttGrpcClient with an mqtt client that's not connected and verify that using a service with it
        //will cause UNAVAILABLE exceptions
        MqttGrpcClient mgClient = new MqttGrpcClient(new MqttAsyncClient("tcp://localhost:1884", Base64Uuid.id()), DEVICE);
        StatusException ex = assertThrows(StatusException.class, () -> mgClient.init());
        assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
        HelloStub stub = new HelloStub(mgClient, SERVICE_NAME);

        //Even though the init failed try to use the client anyway and verify that we get expected failures
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        stub.sayHello(joe, waiter);

        StatusRuntimeException sre = assertThrows(StatusRuntimeException.class,
                () -> waiter.getSingle());

        assertEquals(Status.Code.UNAVAILABLE, sre.getStatus().getCode());

        //Run the same test for StreamIterator
        StreamIterator<HelloReply> iter = new StreamIterator<>();
        stub.sayHello(joe, iter);
        sre = assertThrows(StatusRuntimeException.class,
                () -> iter.next());
        assertEquals(Status.Code.UNAVAILABLE, sre.getStatus().getCode());

    }

    @Test
    public void testNoInitialServerBrokerConnection() throws MqttException {

        //Try initialising the client with a valid mqtt connection and verify it fails because the
        //server is not started
        final MqttAsyncClient clientMqtt = MqttUtils.makeClient(null);
        MqttGrpcClient mgClient = new MqttGrpcClient(clientMqtt, DEVICE);
        StatusException ex = assertThrows(StatusException.class, () -> mgClient.init());
        assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
        assertEquals("Server unavailable", ex.getStatus().getDescription());
        mgClient.close();
    }

    @Test
    public void testServerClosedDuringActivity() throws MqttException, StatusException {

        //Verify that if the server is closed after there has been a successful interaction
        //between client and server then the server will get an UNAVAILABLE message
        //if it tries to make another call.

        //Start the server
        final MqttAsyncClient serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE));
        MqttGrpcServer mgServer = new MqttGrpcServer(serverMqtt, DEVICE);
        mgServer.init();
        final HelloService service = new HelloService();
        HelloSkeleton skeleton = new HelloSkeleton(service);
        mgServer.subscribeService(SERVICE_NAME, skeleton);


        final MqttAsyncClient clientMqtt = MqttUtils.makeClient(null);
        MqttGrpcClient mgClient = new MqttGrpcClient(clientMqtt, DEVICE);
        mgClient.init();
        HelloStub stub = new HelloStub(mgClient, SERVICE_NAME);
        //send a message and make sure we get a response
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>();
        stub.sayHello(joe, waiter);
        HelloReply reply = waiter.getSingle();
        assertEquals("Hello joe", reply.getMessage());

        //Close should be called on a server when the system shuts down
        //close() will then send a message to the LWT topic so that clients can release any resources
        //that they have and so that any open streams get an error.
        //
        mgServer.close();

        StreamWaiter waiter2 = new StreamWaiter<>(5000);
        stub.sayHello(joe, waiter2);

        StatusRuntimeException sre = assertThrows(StatusRuntimeException.class,
                () -> waiter2.getSingle());

        assertEquals(Status.Code.UNAVAILABLE, sre.getStatus().getCode());

        //Run the same test for StreamIterator
        StreamIterator<HelloReply> iter = new StreamIterator<>();
        stub.sayHello(joe, iter);
        sre = assertThrows(StatusRuntimeException.class,
                () -> iter.next());
        assertEquals(sre.getStatus().getCode(), Status.Code.UNAVAILABLE);

    }
*/


}
