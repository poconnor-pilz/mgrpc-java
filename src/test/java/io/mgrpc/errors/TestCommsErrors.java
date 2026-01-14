package io.mgrpc.errors;

import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelBuilder;
import io.mgrpc.mqtt.MqttServerBuilder;
import io.mgrpc.mqtt.MqttUtils;
import io.mgrpc.utils.StatusObserver;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCommsErrors {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        serverMqtt = MqttUtils.makeClient();
        clientMqtt = MqttUtils.makeClient();
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

        @Override
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            responseObserver.onNext(HelloReply.newBuilder().setMessage("hi").build());
            responseObserver.onCompleted();
        }
    }


    @Test
    public void testServerNeverConnected() throws Exception{

        //Verify that if a server is not connected then a call will fail with an UNAVAILABLE error

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();

        MessageChannel baseChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        StatusObserver statusObserver = new StatusObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, statusObserver);

        checkStatus(Status.UNAVAILABLE, statusObserver.waitForStatus(2, TimeUnit.SECONDS));
        baseChannel.close();

    }

    @Test
    public void testServerConnectedButNotInitially() throws Exception{

        //Verify that a call succeeds even if the server is connected sometime after the channel
        //but before the call is made

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();

        MessageChannel baseChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
         Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        StatusObserver statusObserver = new StatusObserver("obs");

        //Allow enough time for the initial ping to the server to fail
        Thread.sleep(500);

        //The server will send a connected status when it starts up
        MessageServer server = new MqttServerBuilder().setClient(serverMqtt).setTopic(serverName).build();
        server.start();
        server.addService(new HelloService());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.sayHello(joe, statusObserver);

        checkStatus(Status.OK, statusObserver.waitForStatus(2, TimeUnit.SECONDS));
        baseChannel.close();
        server.close();

    }


    @Test
    public void testServerClosedMidstream() throws Exception{

        //Close a server while it is streaming responses
        //The broker should send a ConnectionStatus false message. The channel should react to this by sending an UNVAILABLE error
        //to the ongoing call on the client side.


        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();

        MessageChannel baseChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        MqttAsyncClient serverMqttWithLwt = MqttUtils.makeClient();
        MessageServer server = new MqttServerBuilder().setClient(serverMqttWithLwt).setTopic(serverName).build();
        server.start();
        server.addService(new HelloService());

        StatusObserver statusObserver = new StatusObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, statusObserver);

        Thread.sleep(500);
        server.close();
        checkStatus(Status.UNAVAILABLE, statusObserver.waitForStatus(2, TimeUnit.SECONDS));

        baseChannel.close();

    }


    @Test
    public void testServerLWTMidstream() throws Exception{

        //Disconnect a server while it is streaming responses
        //The broker should send an LWT message. The channel should react to this by sending an UNVAILABLE error
        //to the ongoing call on the client side.

        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();

        MessageChannel baseChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        final String statusTopic = MessageServer.getStatusTopic(serverName, "/");
        CloseableSocketFactory sf = new CloseableSocketFactory();
        MqttAsyncClient serverMqttWithLwt = MqttUtils.makeClient(statusTopic, null, sf);
        MessageServer server = new MqttServerBuilder().setClient(serverMqttWithLwt).setTopic(serverName).build();
        server.start();
        server.addService(new HelloService());

        StatusObserver statusObserver = new StatusObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, statusObserver);

        Thread.sleep(500);
        assertEquals(1, baseChannel.getStats().getActiveCalls());
        //This should cause an LWT message which will send on the disconnected status
        sf.disableAndCloseAll();
        checkStatus(Status.UNAVAILABLE, statusObserver.waitForStatus(2, TimeUnit.SECONDS));
        //Calls should be cleaned up because the server is out of contact.
        assertEquals(0, baseChannel.getStats().getActiveCalls());

        baseChannel.close();
        server.close();

    }

    @Test
    void testChannelClosedMidStream() throws Exception {
        //Verify that when the channel is close then the server receives a cancel
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //(The above two are done by the grpc in the code for StreamingServerCallListener.onCancel())
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();
        final String channelStatusTopic = "mgrpc/channelStatus";

        final ListenForCancel listenForCancel = new ListenForCancel();
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setTopic(serverName)
                .setChannelStatusTopic(channelStatusTopic).build();
        server.start();
        server.addService(listenForCancel);

        MessageChannel baseChannel = new MqttChannelBuilder()
                .setClient(clientMqtt)
                .setChannelStatusTopic(channelStatusTopic).build();
        Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(new StatusObserver("test"));
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        listenForCancel.statusObserver.waitForNext(2, TimeUnit.SECONDS);
        assertEquals(server.getStats().getActiveCalls(), 1);

        //Close the channel. The server cancel handler should get called
        baseChannel.close();

        //Verify that the Context.CancellationListener gets called
        //assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        checkStatus(Status.CANCELLED, listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS));

        //The call should be cleaned up on the server.
        assertEquals(server.getStats().getActiveCalls(), 0);

        baseChannel.close();
        server.close();

    }

    @Test
    void testChannelLWTMidStream() throws Exception {
        //Verify that when the channel is closed then the server receives a cancel
        //the server cancel handler will be called and the service will receive an
        //onError with io.grpc.StatusRuntimeException: CANCELLED: client cancelled
        //(The above two are done by the grpc in the code for StreamingServerCallListener.onCancel())
        //The client CancelableObserver will receive onError() with
        //io.grpc.StatusRuntimeException: CANCELLED: tryit

        //Make unique server name for each test to prevent stray status messages from previous tests affecting this test
        final String serverName = Id.shortRandom();

        final ListenForCancel listenForCancel = new ListenForCancel();
        final String channelStatusTopic = "mgrpc/channelStatus";
        MessageServer server = new MqttServerBuilder()
                .setClient(serverMqtt)
                .setTopic(serverName)
                .setChannelStatusTopic(channelStatusTopic).build();
        server.start();
        server.addService(listenForCancel);

        CloseableSocketFactory sf = new CloseableSocketFactory();
        String channelId = Id.random();
        MqttAsyncClient clientMqttWithLwt = MqttUtils.makeClient(channelStatusTopic, channelId, sf);
        MessageChannel baseChannel = new MqttChannelBuilder()
                .setClient(clientMqttWithLwt)
                .setChannelStatusTopic(channelStatusTopic)
                .setChannelId(channelId)
                .build();

        Channel channel = TopicInterceptor.intercept(baseChannel, serverName);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(new StatusObserver("test"));
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        listenForCancel.statusObserver.waitForNext(2, TimeUnit.SECONDS);
        assertEquals(server.getStats().getActiveCalls(), 1);

        //Break the client connection. The broker should send the LWT and the server cancel handler should get called
        sf.disableAndCloseAll();

        //Verify that the Context.CancellationListener gets called
        //assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        checkStatus(Status.CANCELLED, listenForCancel.statusObserver.waitForStatus(10, TimeUnit.SECONDS));

        //The call should be cleaned up on the server.
        assertEquals(server.getStats().getActiveCalls(), 0);

        baseChannel.close();
        server.close();

    }


    private void checkStatus(Status expected, Status actual){
        assertEquals(expected.getCode(), actual.getCode());
    }

}
