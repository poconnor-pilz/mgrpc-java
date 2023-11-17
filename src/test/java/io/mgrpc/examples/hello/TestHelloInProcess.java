package io.mgrpc.examples.hello;

import io.mgrpc.Id;
import io.mgrpc.InProcessMessageTransport;
import io.mgrpc.MessageChannel;
import io.mgrpc.MessageServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;


public class TestHelloInProcess extends TestHelloBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    MessageChannel channel;
    MessageServer server;


    //Make server name short but random to prevent stray status messages from previous tests affecting this test
    private static final String SERVER = Id.shortRandom();

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
