package io.mgrpc.jms;

import io.grpc.Channel;
import io.grpc.examples.helloworld.ExampleHelloServiceGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.examples.hello.HelloServiceForTest;
import io.mgrpc.utils.ToList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test base flow control with jms i.e. set up a jms conduit with broker flow control enabled
 * but then verify that base flow control (with Flow messages, credit and backpressure) will work
 * in this setup
 */
public class TestJmsBaseFlowControl {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Connection serverConnection;
    private static Connection clientConnection;

    @BeforeAll
    public static void startClients() throws Exception {
        EmbeddedBroker.start();
        InitialContext initialContext = new InitialContext();
        ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

        serverConnection = cf.createConnection();
        serverConnection.start();
        clientConnection = cf.createConnection();
        clientConnection.start();
    }

    @AfterAll
    public static void stopClients() throws JMSException {
        serverConnection.close();
        clientConnection.close();
    }

    @Test
    public void testServerStream() throws Exception {
        final String serverId = Id.shortRandom();

        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setTopic(serverId).build();

        server.start();

        MessageChannel messageChannel = new JmsChannelBuilder()
                .setConnection(clientConnection)
                .setUseBrokerFlowControl(true)
                .setFlowCredit(10)
                .setQueueSize(1000)
                .build();

        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        server.addService(new HelloServiceForTest());

        final ExampleHelloServiceGrpc.ExampleHelloServiceBlockingStub stub = ExampleHelloServiceGrpc
                .newBlockingStub(channel)
                .withDeadlineAfter(60*1000, TimeUnit.MILLISECONDS);

        final int numReplies = 30;

        HelloRequest request = HelloRequest.newBuilder()
                .setName("test")
                .setNumResponses(numReplies)
                .build();
        List<HelloReply> responseList = ToList.toList(stub.lotsOfReplies(request));
        assertEquals(numReplies, responseList.size());

        for(int i = 0; i < numReplies; i++) {
            assertEquals("Hello test " + i, responseList.get(i).getMessage());
        }

        messageChannel.close();
        server.close();

    }


    private static class ClientStreamService extends ExampleHelloServiceGrpc.ExampleHelloServiceImplBase{
        public List<String> names;

        public String id = Id.random();

        private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        public ClientStreamService(){
            names = new ArrayList<>();
        }
        @Override
        public StreamObserver<HelloRequest> lotsOfGreetings(StreamObserver< HelloReply > responseObserver) {
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest value) {
                    log.debug("lotsOfGreetings received " + value.getName());
                    names.add(value.getName());
                }
                @Override
                public void onError(Throwable t) {
                    log.error("Error in client stream", t);
                }
                @Override
                public void onCompleted() {
                    HelloReply reply = HelloReply.newBuilder().setMessage("test response").build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            };
        }
    }

    @Test
    public void testClientStream() throws Exception {
        final String serverId = Id.shortRandom();


        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setFlowCredit(10)
                .setTopic(serverId).build();

        server.start();

        MessageChannel messageChannel = new JmsChannelBuilder()
                .setConnection(clientConnection)
                .setUseBrokerFlowControl(true)
                .setFlowCredit(10)
                .setQueueSize(1000)
                .build();

        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        ClientStreamService clientStreamService = new ClientStreamService();

        server.addService(clientStreamService);

        final ExampleHelloServiceGrpc.ExampleHelloServiceStub stub = ExampleHelloServiceGrpc.newStub(channel);

        StreamWaiter<HelloReply> waiter = new StreamWaiter(60*1000);
        StreamObserver<HelloRequest> clientStreamObserver = stub.lotsOfGreetings(waiter);

        final int numRequests = 30;
        for (int i = 0; i < numRequests; i++) {
            clientStreamObserver.onNext(HelloRequest.newBuilder().setName("request " + i).build());
        }
        clientStreamObserver.onCompleted();

        final HelloReply single = waiter.getSingle(); // will block iuntil all names are recieved

        for (int i = 0; i < numRequests; i++) {
            assertEquals("request " + i, clientStreamService.names.get(i));
        }

        messageChannel.close();
        server.close();

    }

}
