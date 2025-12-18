package io.mgrpc.errors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloCustomError;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelBuilder;
import io.mgrpc.mqtt.MqttServerBuilder;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestErrors {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private Channel channel;
    private MessageChannel messageChannel;
    private MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();


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


    @BeforeEach
    void setup() throws Exception {

        //Set up the server
        server = new MqttServerBuilder().setClient(serverMqtt).setTopic(SERVER).build();
        server.start();
        messageChannel = new MqttChannelBuilder().setClient(clientMqtt).build();
        channel = TopicInterceptor.intercept(messageChannel, SERVER);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.close();
    }

    public void checkForLeaks(int numActiveCalls) {
        try {
            //Give the channel and server time to process messages and release resources
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(numActiveCalls, messageChannel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, server.getStats().getActiveCalls());
    }

    @Test
    public void testSingleResponseWithError() throws InterruptedException {
        class SingleResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new SingleResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.sayHello(joe, new NoopStreamObserver<HelloReply>() {
            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
        checkForLeaks(0);
    }


    @Test
    public void testSingleResponseWithException() throws InterruptedException {
        class SingleResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                throw new RuntimeException("an exception message");
            }
        }
        server.addService(new SingleResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.sayHello(joe, new NoopStreamObserver<HelloReply>() {
            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.UNKNOWN);
        assertEquals("an exception message", status.getDescription());
        checkForLeaks(0);
    }


    @Test
    public void testSingleResponseWithBlockingStub() throws InterruptedException {

        class SingleResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new SingleResponseWithError());
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.OUT_OF_RANGE, status.getCode());
        assertEquals("the value is out of range", status.getDescription());
        checkForLeaks(0);

    }

    @Test
    public void testMultiResponseWithError() throws InterruptedException {

        class MultiResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                responseObserver.onNext(reply);
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new MultiResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.lotsOfReplies(joe, new NoopStreamObserver<HelloReply>() {
            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
        checkForLeaks(0);
    }

    @Test
    public void testMultiResponseWithException() throws InterruptedException {

        class MultiResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                responseObserver.onNext(reply);
                throw new RuntimeException("an exception message");
            }
        }
        server.addService(new MultiResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.lotsOfReplies(joe, new NoopStreamObserver<HelloReply>() {
            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException) ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.UNKNOWN);
        assertEquals("an exception message", status.getDescription());
        checkForLeaks(0);
    }


    @Test
    public void testMultiResponseWithErrorBlockingStub() {

        class MultiResponseWithError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void lotsOfReplies(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
                responseObserver.onNext(reply);
                Status status = Status.OUT_OF_RANGE.withDescription("the value is out of range");
                responseObserver.onError(status.asRuntimeException());
            }
        }
        server.addService(new MultiResponseWithError());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Iterator<HelloReply> iter = stub.lotsOfReplies(joe);
        assertEquals("Hello joe", iter.next().getMessage());
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> iter.next());

        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
        checkForLeaks(0);
    }

    @Test
    public void testSingleResponseWithRichError() throws InvalidProtocolBufferException {

        class SingleResponseWithRichError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.OUT_OF_RANGE.getNumber())
                        .setMessage("the value is out of range")
                        .addDetails(Any.pack(ErrorInfo.newBuilder()
                                .setReason("test failed")
                                .setDomain("com.pilz.errors")
                                .putMetadata("somekey", "somevalue")
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }
        server.addService(new SingleResponseWithRichError());
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());

        //The io.grpc.status above is exactly the same as when in testSingleResponseWithError
        //To get the rich error we convert to com.google.rpc.status
        //The details are in an google.rpc.ErrorInfo
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        //Note that the line below could also use StatusProto.fromThrowable(statusRuntimeException)
        com.google.rpc.Status rpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        assertEquals(Code.OUT_OF_RANGE, Code.forNumber(rpcStatus.getCode()));
        assertEquals("the value is out of range", rpcStatus.getMessage());
        for (Any any : rpcStatus.getDetailsList()) {
            if (any.is(ErrorInfo.class)) {
                ErrorInfo errorInfo = any.unpack(ErrorInfo.class);
                assertEquals("test failed", errorInfo.getReason());
                assertEquals("com.pilz.errors", errorInfo.getDomain());
                assertEquals("somevalue", errorInfo.getMetadataMap().get("somekey"));
            }
        }
        checkForLeaks(0);
    }


    @Test
    public void testSingleResponseWithRichCustomError() throws InvalidProtocolBufferException {

        class SingleResponseWithRichCustomError extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
                //Put in a HelloCustomError defined in helloworld.proto
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.OUT_OF_RANGE.getNumber())
                        .setMessage("the value is out of range")
                        .addDetails(Any.pack(HelloCustomError.newBuilder()
                                .setHelloErrorCode(20)
                                .setHelloErrorDescription("an error description")
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }
        server.addService(new SingleResponseWithRichCustomError());
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, () -> stub.sayHello(joe));
        Status status = statusRuntimeException.getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());

        //The io.grpc.status above is exactly the same as when in testSingleResponseWithError
        //To get the rich error we convert to com.google.rpc.status
        //Usually to represent details it will be enough  to use one of the protocol buffers in
        //https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
        //described also at https://cloud.google.com/apis/design/errors
        //But in this case the details are in a HelloCustomError
        com.google.rpc.Status rpcStatus = StatusProto.fromStatusAndTrailers(status, null);
        assertEquals(Code.OUT_OF_RANGE, Code.forNumber(rpcStatus.getCode()));
        assertEquals("the value is out of range", rpcStatus.getMessage());
        for (Any any : rpcStatus.getDetailsList()) {
            if (any.is(HelloCustomError.class)) {
                HelloCustomError helloCustomError = any.unpack(HelloCustomError.class);
                assertEquals(20, helloCustomError.getHelloErrorCode());
                assertEquals("an error description", helloCustomError.getHelloErrorDescription());
            }
        }
        checkForLeaks(0);
    }

    @Test
    public void testHelloWithNoValue() throws Exception {

        class OnlySendsCompleted extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase {
            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloReply> singleResponse){
                //Send completed without first sending onNext()
                //No service should do this but it *can* do it so we need to test for this case
                singleResponse.onCompleted();
            }
        }
        server.addService(new OnlySendsCompleted());
        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub blockingStub
                = ExampleHelloServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS);;
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> blockingStub.sayHello(joe));
        //The call should timeout
        assertEquals(Status.DEADLINE_EXCEEDED.getCode(), ex.getStatus().getCode());
    }


}


