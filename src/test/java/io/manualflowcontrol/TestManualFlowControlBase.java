package io.manualflowcontrol;

import io.grpc.Status;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public abstract class TestManualFlowControlBase {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    public abstract MessageChannel getChannel();

    public abstract MessageServer getServer();

    @Test
    public void testManualFlowControl() throws Exception{

        // Service class implementation
        ExampleHelloServiceGrpc.ExampleHelloServiceImplBase svc = new ExampleHelloServiceGrpc.ExampleHelloServiceImplBase() {
            @Override
            public StreamObserver<HelloRequest> bidiHello(final StreamObserver<HelloReply> responseObserver) {
                // Set up manual flow control for the request stream. It feels backwards to configure the request
                // stream's flow control using the response stream's observer, but this is the way it is.
                final ServerCallStreamObserver<HelloReply> serverCallStreamObserver =
                        (ServerCallStreamObserver<HelloReply>) responseObserver;
                serverCallStreamObserver.disableAutoRequest();

                // Set up a back-pressure-aware consumer for the request stream. The onReadyHandler will be invoked
                // when the consuming side has enough buffer space to receive more messages.
                //
                // Note: the onReadyHandler's invocation is serialized on the same thread pool as the incoming StreamObserver's
                // onNext(), onError(), and onComplete() handlers. Blocking the onReadyHandler will prevent additional messages
                // from being processed by the incoming StreamObserver. The onReadyHandler must return in a timely manner or
                // else message processing throughput will suffer.
                class OnReadyHandler implements Runnable {
                    // Guard against spurious onReady() calls caused by a race between onNext() and onReady(). If the transport
                    // toggles isReady() from false to true while onNext() is executing, but before onNext() checks isReady(),
                    // request(1) would be called twice - once by onNext() and once by the onReady() scheduled during onNext()'s
                    // execution.
                    private boolean wasReady = false;

                    @Override
                    public void run() {
                        if (serverCallStreamObserver.isReady() && !wasReady) {
                            wasReady = true;
                            logger.info("READY");
                            // Signal the request sender to send one message. This happens when isReady() turns true, signaling that
                            // the receive buffer has enough free space to receive more messages. Calling request() serves to prime
                            // the message pump.
                            serverCallStreamObserver.request(1);
                        }
                    }
                }
                final OnReadyHandler onReadyHandler = new OnReadyHandler();
                serverCallStreamObserver.setOnReadyHandler(onReadyHandler);

                // Give gRPC a StreamObserver that can observe and process incoming requests.
                return new StreamObserver<HelloRequest>() {
                    @Override
                    public void onNext(HelloRequest request) {
                        // Process the request and send a response or an error.
                        try {
                            // Accept and enqueue the request.
                            String name = request.getName();
                            logger.info("--> " + name);

                            // Simulate server "work"
                            Thread.sleep(100);

                            // Send a response.
                            String message = "Hello " + name;
                            logger.info("<-- " + message);
                            HelloReply reply = HelloReply.newBuilder().setMessage(message).build();
                            responseObserver.onNext(reply);

                            // Check the provided ServerCallStreamObserver to see if it is still ready to accept more messages.
                            if (serverCallStreamObserver.isReady()) {
                                // Signal the sender to send another request. As long as isReady() stays true, the server will keep
                                // cycling through the loop of onNext() -> request(1)...onNext() -> request(1)... until the client runs
                                // out of messages and ends the loop (via onCompleted()).
                                //
                                // If request() was called here with the argument of more than 1, the server might runs out of receive
                                // buffer space, and isReady() will turn false. When the receive buffer has sufficiently drained,
                                // isReady() will turn true, and the serverCallStreamObserver's onReadyHandler will be called to restart
                                // the message pump.
                                serverCallStreamObserver.request(1);
                            } else {
                                // If not, note that back-pressure has begun.
                                onReadyHandler.wasReady = false;
                            }
                        } catch (Throwable throwable) {
                            throwable.printStackTrace();
                            responseObserver.onError(
                                    Status.UNKNOWN.withDescription("Error handling request").withCause(throwable).asException());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        // End the response stream if the client presents an error.
                        t.printStackTrace();
                        responseObserver.onCompleted();
                    }

                    @Override
                    public void onCompleted() {
                        // Signal the end of work when the client ends the request stream.
                        logger.info("COMPLETED");
                        responseObserver.onCompleted();
                    }
                };
            }
        };


        getServer().addService(svc);

        final CountDownLatch done = new CountDownLatch(1);


        ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(getChannel());

        // When using manual flow-control and back-pressure on the client, the ClientResponseObserver handles both
        // request and response streams.
        ClientResponseObserver<HelloRequest, HelloReply> clientResponseObserver =
                new ClientResponseObserver<HelloRequest, HelloReply>() {

                    ClientCallStreamObserver<HelloRequest> requestStream;

                    @Override
                    public void beforeStart(final ClientCallStreamObserver<HelloRequest> requestStream) {
                        this.requestStream = requestStream;
                        // Set up manual flow control for the response stream. It feels backwards to configure the response
                        // stream's flow control using the request stream's observer, but this is the way it is.
                        requestStream.disableAutoRequestWithInitial(1);

                        // Set up a back-pressure-aware producer for the request stream. The onReadyHandler will be invoked
                        // when the consuming side has enough buffer space to receive more messages.
                        //
                        // Messages are serialized into a transport-specific transmit buffer. Depending on the size of this buffer,
                        // MANY messages may be buffered, however, they haven't yet been sent to the server. The server must call
                        // request() to pull a buffered message from the client.
                        //
                        // Note: the onReadyHandler's invocation is serialized on the same thread pool as the incoming
                        // StreamObserver's onNext(), onError(), and onComplete() handlers. Blocking the onReadyHandler will prevent
                        // additional messages from being processed by the incoming StreamObserver. The onReadyHandler must return
                        // in a timely manner or else message processing throughput will suffer.
                        requestStream.setOnReadyHandler(new Runnable() {
                            // An iterator is used so we can pause and resume iteration of the request data.
                            Iterator<String> iterator = names().iterator();

                            @Override
                            public void run() {
                                // Start generating values from where we left off on a non-gRPC thread.
                                while (requestStream.isReady()) {
                                    if (iterator.hasNext()) {
                                        // Send more messages if there are more messages to send.
                                        String name = iterator.next();
                                        logger.info("--> " + name);
                                        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
                                        requestStream.onNext(request);
                                    } else {
                                        // Signal completion if there is nothing left to send.
                                        requestStream.onCompleted();
                                    }
                                }
                            }
                        });
                    }

                    @Override
                    public void onNext(HelloReply value) {
                        logger.info("<-- " + value.getMessage());
                        // Signal the sender to send one message.
                        requestStream.request(1);
                    }

                    @Override
                    public void onError(Throwable t) {
                        t.printStackTrace();
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        logger.info("All Done");
                        done.countDown();
                    }
                };

        // Note: clientResponseObserver is handling both request and response stream processing.
        stub.bidiHello(clientResponseObserver);

        done.await();


    }

    private static List<String> names() {
        return Arrays.asList(
                "Sophia",
                "Jackson",
                "Emma",
                "Aiden",
                "Olivia",
                "Lucas",
                "Ava",
                "Liam",
                "Mia",
                "Noah",
                "Isabella",
                "Ethan",
                "Riley",
                "Mason",
                "Aria",
                "Caden",
                "Zoe",
                "Oliver",
                "Charlotte",
                "Elijah",
                "Lily",
                "Grayson",
                "Layla",
                "Jacob",
                "Amelia",
                "Michael",
                "Emily",
                "Benjamin",
                "Madelyn",
                "Carter",
                "Aubrey",
                "James",
                "Adalyn",
                "Jayden",
                "Madison",
                "Logan",
                "Chloe",
                "Alexander",
                "Harper",
                "Caleb",
                "Abigail",
                "Ryan",
                "Aaliyah",
                "Luke",
                "Avery",
                "Daniel",
                "Evelyn",
                "Jack",
                "Kaylee",
                "William",
                "Ella",
                "Owen",
                "Ellie",
                "Gabriel",
                "Scarlett",
                "Matthew",
                "Arianna",
                "Connor",
                "Hailey",
                "Jayce",
                "Nora",
                "Isaac",
                "Addison",
                "Sebastian",
                "Brooklyn",
                "Henry",
                "Hannah",
                "Muhammad",
                "Mila",
                "Cameron",
                "Leah",
                "Wyatt",
                "Elizabeth",
                "Dylan",
                "Sarah",
                "Nathan",
                "Eliana",
                "Nicholas",
                "Mackenzie",
                "Julian",
                "Peyton",
                "Eli",
                "Maria",
                "Levi",
                "Grace",
                "Isaiah",
                "Adeline",
                "Landon",
                "Elena",
                "David",
                "Anna",
                "Christian",
                "Victoria",
                "Andrew",
                "Camilla",
                "Brayden",
                "Lillian",
                "John",
                "Natalie",
                "Lincoln"
        );
    }
}
