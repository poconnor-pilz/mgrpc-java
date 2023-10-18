package io.mgrpc.errors;

import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelMessageTransport;
import io.mgrpc.mqtt.MqttServerServerMessageTransport;
import io.mgrpc.utils.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestCommsErrors {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shrt(Id.random());


    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
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

        //Verify that if a server is not connected then a call will fail with an UNAVAILABLE error

        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqtt, SERVER));
        channel.start();
        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        assertTrue(errorObserver.errorLatch.await(2, TimeUnit.SECONDS));
        assertEquals(Status.UNAVAILABLE.getCode().value(), errorObserver.exception.getStatus().getCode().value());
        channel.close();

    }

    @Test
    public void testServerConnectedButNotInitially() throws Exception{

        //Verify that a call succeeds even if the server is connected sometime after the channel
        //but before the call is made
        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqtt, SERVER));
        channel.start();
        ErrorObserver errorObserver = new ErrorObserver("obs");

        //Allow enough time for the initial ping to the server to fail
        Thread.sleep(500);

        //The server will send a connected status when it starts up
        MessageServer server = new MessageServer(new MqttServerServerMessageTransport(serverMqtt, SERVER));
        server.start();
        server.addService(new HelloService());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        assertFalse(errorObserver.errorLatch.await(2, TimeUnit.SECONDS));
        channel.close();
        server.close();

    }


    @Test
    public void testServerClosedMidstream() throws Exception{

        //Close a server while it is streaming responses
        //The broker should send an LWT message. The channel should react to this by sending an UNVAILABLE error
        //to the ongoing call on the client side.


        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up
        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqtt, SERVER));
        channel.start();
        MqttAsyncClient serverMqttWithLwt = MqttUtils.makeClient(new ServerTopics(SERVER, "/").status);
        MessageServer server = new MessageServer(new MqttServerServerMessageTransport(serverMqttWithLwt, SERVER));
        server.start();
        server.addService(new HelloService());

        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        Thread.sleep(500);
        server.close();
        assertTrue(errorObserver.errorLatch.await(2, TimeUnit.SECONDS));
        assertEquals(Status.UNAVAILABLE.getCode().value(), errorObserver.exception.getStatus().getCode().value());

        channel.close();

    }


    @Test
    public void testServerLWTMidstream() throws Exception{

        //Disconnect a server while it is streaming responses
        //The broker should send an LWT message. The channel should react to this by sending an UNVAILABLE error
        //to the ongoing call on the client side.

        //Note that in http grpc if the server is shutdown while streaming to a client
        //then the client will not receive an error. This may be because the server may re-connect and continue
        //In our case the channel will send an error and all client calls will be cleaned up
        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqtt, SERVER));
        channel.start();
        final String statusTopic = new ServerTopics(SERVER, "/").status;
        CloseableSocketFactory sf = new CloseableSocketFactory();
        MqttAsyncClient serverMqttWithLwt = MqttUtils.makeClient(statusTopic, sf);
        MessageServer server = new MessageServer(new MqttServerServerMessageTransport(serverMqttWithLwt, SERVER));
        server.start();
        server.addService(new HelloService());

        ErrorObserver errorObserver = new ErrorObserver("obs");

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        stub.lotsOfReplies(joe, errorObserver);

        Thread.sleep(500);
        assertEquals(1, channel.getStats().getActiveCalls());
        //This should cause an LWT message which will send on the disconnected status
        sf.disableAndCloseAll();
        assertTrue(errorObserver.errorLatch.await(2, TimeUnit.SECONDS));
        //Calls should be cleaned up because the server is out of contact.
        assertEquals(0, channel.getStats().getActiveCalls());
        assertEquals(Status.UNAVAILABLE.getCode().value(), errorObserver.exception.getStatus().getCode().value());

        channel.close();
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

        final ListenForCancel listenForCancel = new ListenForCancel();
        MessageServer server = new MessageServer(new MqttServerServerMessageTransport(serverMqtt, SERVER));
        server.start();
        server.addService(listenForCancel);

        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqtt, SERVER));
        channel.start();

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(new ErrorObserver("test"));
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        listenForCancel.errorObserver.waitForNext(2, TimeUnit.SECONDS);
        assertEquals(server.getStats().getActiveCalls(), 1);

        //Close the channel. The server cancel handler should get called
        channel.close();

        //Verify that the Context.CancellationListener gets called
        //assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.errorObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //The call should be cleaned up on the server.
        assertEquals(server.getStats().getActiveCalls(), 0);

        channel.close();
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

        final ListenForCancel listenForCancel = new ListenForCancel();
        MessageServer server = new MessageServer(new MqttServerServerMessageTransport(serverMqtt, SERVER));
        server.start();
        server.addService(listenForCancel);

        CloseableSocketFactory sf = new CloseableSocketFactory();
        String channelId = Id.random();
        String clientStatusTopic = new ServerTopics(SERVER, "/").statusClients + "/" + channelId;
        MqttAsyncClient clientMqttWithLwt = MqttUtils.makeClient(clientStatusTopic, sf);
        MessageChannel channel = new MessageChannel(new MqttChannelMessageTransport(clientMqttWithLwt, SERVER), channelId);
        channel.start();

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();

        final StreamObserver<HelloRequest> inStream = stub.bidiHello(new ErrorObserver("test"));
        inStream.onNext(joe);

        //Wait for at least one message to go through before canceling to make sure the call is fully started.
        listenForCancel.errorObserver.waitForNext(2, TimeUnit.SECONDS);
        assertEquals(server.getStats().getActiveCalls(), 1);

        //Break the client connection. The broker should send the LWT and the server cancel handler should get called
        sf.disableAndCloseAll();

        //Verify that the Context.CancellationListener gets called
        //assertTrue(listenForCancel.contextListenerCancelled.await(5, TimeUnit.SECONDS));

        //The client stream returned by the server should receive a onError() CANCELLED
        assertEquals(listenForCancel.errorObserver.waitForStatus(10, TimeUnit.SECONDS).getCode(), Status.CANCELLED.getCode());

        //The call should be cleaned up on the server.
        assertEquals(server.getStats().getActiveCalls(), 0);

        channel.close();
        server.close();

    }



}
