package io.mgrpc.jms;

import io.grpc.Channel;
import io.grpc.Status;
import io.mgrpc.*;
import io.mgrpc.examples.hello.FlowControlTests;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test with and without broker call queues to verify that when broker call queues are used
 * flow control is not used then the calls work correctly because the broker queues
 * messages until they are needed, making flow control unnecessary.
 */
public class TestJmsFlowControlUsingBrokerCallQueues {

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
    public void testServerQueueCapacityExceeded() throws Exception {

        //Verify that when broker flow control is not set then it is possible to make the internal server
        //buffer/queue overflow

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setQueueSize(10)
                .setFlowCredit(Integer.MAX_VALUE) //no effective base flow control
                .setTopic(serverId).build();

        server.start();

        //Set up a channel without broker flow control
        MessageChannel messageChannel = new MessageChannel(new JmsChannelConduit(clientConnection, false));
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testServerQueueCapacityExceeded(server, channel);

        messageChannel.close();
        server.close();
    }


    @Test
    public void testClientQueueCapacityExceeded() throws Exception{

        final String serverId = Id.shortRandom();

        //Verify that if the server sends a lot of messages to a client that is blocked and there is no
        //broker flow control then the client queue limit is reached.
        //The test code should get an error and the server should get a cancel so that it stops sending messages.
        //and the input stream to the server should get an error.

        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setTopic(serverId).build();
        server.start();

        //Make a channel with queue size 10 without broker flow control and without base flow control
        MessageChannel messageChannel =  new JmsChannelBuilder()
                .setConnection(clientConnection)
                .setQueueSize(10)
                .setFlowCredit(Integer.MAX_VALUE)
                .setUseBrokerCallQueues(false).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testClientQueueCapacityExceeded(server, channel);

        messageChannel.close();
        server.close();

    }

    @Test
    public void testClientStreamFlowControl() throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setQueueSize(10)
                .setFlowCredit(Integer.MAX_VALUE) // no effective base flow control
                .setTopic(serverId).build();
        server.start();

        //Set up a channel with broker flow control
        MessageChannel messageChannel = new MessageChannel(new JmsChannelConduit(clientConnection, true));
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testClientStreamFlowControl(server, channel);

        messageChannel.close();
        server.close();
    }


    @Test
    public void testServerStreamFlowControl() throws Exception {

        //Make a service that blocks until the test flips a latch
        //While the service is blocked try to overlflow the internal MessageServer queue and verify
        //That it doesn't cause a problem because the broker has buffered the messages
        //Then verify that when the service is unblocked it eventually pulls all the messages from the server

        final String serverId = Id.shortRandom();
        //Make a server with queue size 10
        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setQueueSize(10)
                .setFlowCredit(Integer.MAX_VALUE)
                .setTopic(serverId).build();
        server.start();

        //Set up a channel with broker flow control
        MessageChannel messageChannel =  new JmsChannelBuilder()
                .setConnection(clientConnection)
                .setQueueSize(10)
                .setFlowCredit(Integer.MAX_VALUE) //no effective base flow control
                .setUseBrokerCallQueues(true).build();
        Channel channel = TopicInterceptor.intercept(messageChannel, serverId);

        FlowControlTests.testServerStreamFlowControl(server, channel);

        messageChannel.close();
        server.close();
    }



    private void checkStatus(Status expected, Status actual){
        assertEquals(expected.getCode(), actual.getCode());
    }
}
