package io.mgrpc.jms;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.mgrpc.*;
import io.mgrpc.examples.hello.HelloServiceForTest;
import io.mgrpc.examples.hello.TestHelloBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import java.lang.invoke.MethodHandles;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestHelloJms extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Connection serverConnection;
    private static Connection clientConnection;

    Channel channel;
    MessageChannel baseChannel;

    MessageServer server;




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

    @BeforeEach
    void setup() throws Exception{


        //Make server name short but random to prevent stray status messages from previous tests affecting this test
        final String serverTopic = "mgprc/" + Id.shortRandom();

        //Set up the serverb
        server = new MessageServer(new JmsServerConduit(serverConnection, serverTopic));
        server.start();
        server.addService(new HelloServiceForTest());
        Thread.sleep(1000);
        baseChannel = new MessageChannel(new JmsChannelConduit(clientConnection, true));

        channel = ClientInterceptors.intercept(baseChannel, new TopicInterceptor(serverTopic));

    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
        baseChannel.close();

    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void checkNumActiveCalls(int numActiveCalls) {
        assertEquals(numActiveCalls, this.baseChannel.getStats().getActiveCalls());
        assertEquals(numActiveCalls, this.server.getStats().getActiveCalls());
    }

}
