package io.mgrpc.examples.hello;

import io.mgrpc.*;
import io.mgrpc.InProcessMessageTransport;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestHelloMqtt extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    MessageChannel channel;
    MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shrt(Id.random());

    private static final long REQUEST_TIMEOUT = 2000;

    @BeforeEach
    void setup() throws Exception{

        //Set up the serverb
        InProcessMessageTransport transport = new InProcessMessageTransport();
        server = new MessageServer(transport.getServerTransport());
        server.start();
        server.addService(new HelloServiceForTest());
        channel = new MessageChannel(transport.getChannelTransport());
        channel.start();
    }

    @AfterEach
    void tearDown() throws Exception{
        server.close();
        channel.close();
    }


    @Override
    public MessageChannel getChannel() {
        return this.channel;
    }

    @Override
    public MessageServer getServer() {
        return this.server;
    }
}
