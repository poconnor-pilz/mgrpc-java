package io.mgrpc.jms;

import io.grpc.Channel;
import io.mgrpc.EmbeddedBroker;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
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


public class TestHelloJms extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Connection serverConnection;
    private static Connection clientConnection;

    MessageChannel channel;


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
        channel = new JmsChannelBuilder().setConnection(clientConnection).setUseBrokerCallQueues(false).build();
    }

    @AfterEach
    void tearDown() throws Exception{
        channel.close();
    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }


    @Override
    public int getChannelActiveCalls() {
        return this.channel.getStats().getActiveCalls();
    }

    public MessageServer makeMessageServer(String serverTopic) throws Exception {
        MessageServer server = new JmsServerBuilder()
                .setConnection(serverConnection)
                .setTopic(serverTopic).build();
        server.start();
        return server;
    }

}
