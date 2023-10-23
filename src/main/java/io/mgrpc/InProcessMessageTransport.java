package io.mgrpc;

import io.mgrpc.messaging.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class providing server and channel transports that runs in process. Useful for unit testing mgrpc.
 * To do production level in process grpc it would be better to use io.grpc.inprocess.InProcessServerBuilder
 * which will be more efficient.
 */
public class InProcessMessageTransport {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Map<String, ChannelMessageListener> channelsById = new ConcurrentHashMap<>();
    private ServerMessageListener server;

    private final ServerMessageTransport serverTransport = new InprocServerTransport();

    public ChannelMessageTransport getChannelTransport(){
        return new InprocChannelTransport();
    }

    public ServerMessageTransport getServerTransport(){
        return serverTransport;
    }


    private class InprocServerTransport implements ServerMessageTransport {


        @Override
        public void start(ServerMessageListener server) throws MessagingException {
            if(InProcessMessageTransport.this.server != null){
                String err = "InProcessMessageTransport instance can only be associated with one Server";
                log.error(err);
                throw new MessagingException(err);
            }
            InProcessMessageTransport.this.server = server;
        }

        @Override
        public void close() {

        }

        @Override
        public void send(String channelId, String methodName, byte[] buffer) throws MessagingException {
            final ChannelMessageListener channel = channelsById.get(channelId);
            if(channel == null){
                String err = "Channel " + channelId +  " does not exist";
                log.error(err);
                throw new MessagingException(err);
            }
            channel.onMessage(buffer);
        }
    }

    private class InprocChannelTransport implements ChannelMessageTransport {

        private String channelId;

        @Override
        public void start(ChannelMessageListener channel) throws MessagingException {
            this.channelId = channel.getChannelId();
            channelsById.put(channelId, channel);
        }

        @Override
        public void close() {
            channelsById.remove(channelId);
        }

        @Override
        public void send(String methodName, byte[] buffer) throws MessagingException {
            server.onMessage(buffer);
        }
    }

}
