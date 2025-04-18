package io.mgrpc.examples.hello;

import io.grpc.Channel;
import io.mgrpc.InProcessConduit;
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


    @BeforeEach
    void setup() throws Exception{

        //Set up the serverb
        channel = new MessageChannel(InProcessConduit.getInstance().getChannelConduit());
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
        MessageServer server = new MessageServer(InProcessConduit.getInstance().getServerConduit(serverTopic));
        server.start();
        return server;
    }


}
