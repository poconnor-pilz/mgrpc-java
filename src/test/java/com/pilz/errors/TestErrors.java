package com.pilz.errors;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.pilz.examples.hello.HelloServiceForTest;
import com.pilz.mqttgrpc.*;
import com.pilz.utils.MqttUtils;
import io.grpc.*;
import io.grpc.examples.helloworld.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TestErrors {

    private static final Logger log = LoggerFactory.getLogger(TestErrors.class);

    private static MqttAsyncClient serverMqtt;
    private static MqttAsyncClient clientMqtt;

    private MqttChannel channel;
    private MqttServer server;

    private ErrorsService errorsService;



    private static final String DEVICE = "device";
    private static final String SERVICE_NAME = "errorsservice";

    private static final long REQUEST_TIMEOUT = 2000;


    @BeforeAll
    public static void startBrokerAndClients() throws MqttException, IOException {

        MqttUtils.startEmbeddedBroker();

        serverMqtt = MqttUtils.makeClient(Topics.systemStatus(DEVICE), "tcp://localhost:1883");
        clientMqtt = MqttUtils.makeClient(null, "tcp://localhost:1883");
    }

    @AfterAll
    public static void stopClientsAndBroker() throws MqttException {
        serverMqtt.disconnect();
        serverMqtt.close();
        serverMqtt = null;
        clientMqtt.disconnect();
        clientMqtt.close();
        clientMqtt = null;
        MqttUtils.stopEmbeddedBroker();
    }





    @BeforeEach
    void setup() throws Exception{

        //Set up the server
        server = new MqttServer(serverMqtt, DEVICE);
        server.init();
        errorsService = new ErrorsService();
        server.addService(errorsService);
        channel = new MqttChannel(clientMqtt, DEVICE);
        channel.init();
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
    }



    public void testPubSub() throws Exception{
        String topic = "test";
        int[] count = {0};
        serverMqtt.subscribe(topic, 1, new MqttExceptionLogger(new IMqttMessageListener() {
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {

                log.debug("Got message: " + message.toString() );
                if(count[0] == 0) {
                    Thread.sleep(10 * 1000);
                }
                count[0] = 1;
            }
        }));

        int i = 1;
        while(true){
            clientMqtt.publish(topic, new MqttMessage(("test" + i++).getBytes()));
            log.debug("published");
            Thread.sleep(200);
        }
    }

    @Test
    public void testSingleResponseWithError() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.singleResponseWithError(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {}

            @Override
            public void onError(Throwable t) {
               ex[0] = t;
               latch.countDown();
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException)ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testTimeout() throws IOException {

        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor()
                .addService(new ErrorsService() {
                })
                .build().start();
        ManagedChannel inProcChannel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(inProcChannel).withDeadlineAfter(1, TimeUnit.SECONDS);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWith5SecondDelay(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode());
        //assertEquals("the value is out of range", status.getDescription());

    }

    class CancelableObserver implements ClientResponseObserver<HelloRequest, HelloReply>{
        private ClientCallStreamObserver requestStream;
        private final CountDownLatch latch;

        CancelableObserver(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver reqStream) {
            requestStream = reqStream;
        }
        public void cancel(String message){
            requestStream.cancel(message, null);
        }

        @Override
        public void onNext(HelloReply value) {
            log.debug("next");
        }

        @Override
        public void onError(Throwable t) {
            log.debug("Error", t);
            latch.countDown();
        }

        @Override
        public void onCompleted() {
            log.debug("completed");
            latch.countDown();
        }

    }


    @Test
    public void testCancel() throws IOException, InterruptedException {

//        String uniqueName = InProcessServerBuilder.generateName();
//        Server server = InProcessServerBuilder.forName(uniqueName)
//                .directExecutor()
//                .addService(new ErrorsService() {
//                })
//                .build().start();
//        ManagedChannel inProcChannel = InProcessChannelBuilder.forName(uniqueName)
//                .directExecutor()
//                .build();
//


        int port = 50051;
        Server remoteServer = ServerBuilder.forPort(port)
                .addService(new ErrorsService())
                .build()
                .start();

        String target = "localhost:50051";
        ManagedChannel remoteChannel = ManagedChannelBuilder.forTarget(target)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build();

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(remoteChannel);
        CountDownLatch latch = new CountDownLatch(1);

        final Context.CancellableContext[] cancellableContext = {null};




        final CancelableObserver cancelableObserver = new CancelableObserver(latch);
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("Sending cancel");
                cancelableObserver.cancel("tryit");
                //cancellableContext[0].cancel(null);
            }
        });



        stub.cancelDuringServerStream(joe, cancelableObserver);

        log.debug("waiting");



        latch.await();
    }


    @Test
    public void testCancelMqtt() throws IOException, InterruptedException {


        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        CountDownLatch latch = new CountDownLatch(1);

        errorsService.cancelledLatch = new CountDownLatch(1);

        final CancelableObserver cancelableObserver = new CancelableObserver(latch);
        Executors.newSingleThreadExecutor().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.debug("Sending cancel");
                cancelableObserver.cancel("tryit");
            }
        });


        stub.cancelDuringServerStream(joe, cancelableObserver);

        log.debug("waiting");

        assert(latch.await(5, TimeUnit.SECONDS));

        errorsService.cancelledLatch.await();

        log.debug("done");
    }


    @Test
    public void testSingleResponseWithBlockingStub() throws InterruptedException {

        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithError(joe));
        Status status = statusRuntimeException.getStatus();
        assertEquals(Status.Code.OUT_OF_RANGE, status.getCode());
        assertEquals("the value is out of range", status.getDescription());
    }


    @Test
    public void testMultiResponseWithError() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Throwable[] ex = {null};
        CountDownLatch latch = new CountDownLatch(1);
        stub.multiResponseWithError(joe, new StreamObserver<HelloReply>() {
            @Override
            public void onNext(HelloReply value) {}

            @Override
            public void onError(Throwable t) {
                ex[0] = t;
                latch.countDown();
            }

            @Override
            public void onCompleted() {}
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertTrue(ex[0] instanceof StatusRuntimeException);

        Status status = ((StatusRuntimeException)ex[0]).getStatus();

        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }

    @Test
    public void testMultiResponseWithErrorIterator() throws InterruptedException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final Iterator<HelloReply> iter = stub.multiResponseWithError(joe);
        assertEquals("Hello joe", iter.next().getMessage());
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
                () -> iter.next());

        Status status = statusRuntimeException.getStatus();
        assertEquals(status.getCode(), Status.Code.OUT_OF_RANGE);
        assertEquals("the value is out of range", status.getDescription());
    }
    @Test
    public void testErrorInClientStream(){
        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.errorInClientStream(waiter);
        clientStreamObserver.onNext(joe);
        clientStreamObserver.onError(Status.OUT_OF_RANGE.withDescription("some description").asRuntimeException());
        final HelloReply reply = waiter.getSingle();
        assertEquals("ok", reply.getMessage());
    }


    @Test
    public void testSingleResponseWithRichError() throws InterruptedException, InvalidProtocolBufferException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithRichError(joe));
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
    }

    @Test
    public void testSingleResponseWithRichCustomError() throws InterruptedException, InvalidProtocolBufferException {

        final ErrorsServiceGrpc.ErrorsServiceBlockingStub stub = ErrorsServiceGrpc.newBlockingStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class, ()->stub.singleResponseWithRichCustomError(joe));
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
    }

    @Test
    public void testRichErrorInClientStream(){

        final ErrorsServiceGrpc.ErrorsServiceStub stub = ErrorsServiceGrpc.newStub(channel);
        HelloRequest joe = HelloRequest.newBuilder().setName("joe").build();
        StreamWaiter<HelloReply> waiter = new StreamWaiter<>(REQUEST_TIMEOUT);
        StreamObserver<HelloRequest> clientStreamObserver = stub.richErrorInClientStream(waiter);

        com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                .setCode(Code.OUT_OF_RANGE.getNumber())
                .setMessage("the value is out of range")
                .addDetails(Any.pack(ErrorInfo.newBuilder()
                        .setReason("test failed")
                        .setDomain("com.pilz.errors")
                        .putMetadata("somekey", "somevalue")
                        .build()))
                .build();
        clientStreamObserver.onError(StatusProto.toStatusRuntimeException(status));

        final HelloReply reply = waiter.getSingle();
        assertEquals("ok", reply.getMessage());
    }




}
