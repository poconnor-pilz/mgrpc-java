package io.mgrpc.errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCommsErrors {

    private static Logger log = LoggerFactory.getLogger(TestCommsErrors.class);

    private static final String DEVICE = "device1";

    private static final String SERVICE_NAME = "helloservice";

/*
    @BeforeAll
    public static void startBroker() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

    }


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
