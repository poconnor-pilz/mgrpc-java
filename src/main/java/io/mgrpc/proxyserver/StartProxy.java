package io.mgrpc.proxyserver;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.mgrpc.GrpcProxy;
import io.mgrpc.MessageChannel;
import io.mgrpc.mqtt.MqttChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class StartProxy {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            System.out.println("Usage: StartProxy mqtt broker url (e.g. tcp://localhost:1883)");
            return;
        }
        final ClientFactory clientFactory = new ClientFactory(args[0]);
        MessageChannel messageChannel = new MqttChannelBuilder().setClientFactory(clientFactory).build();


        //We want to wire this:
        //HttpServer -> GrpcProxy -> MqttChannel
        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy(messageChannel);
        final GrpcProxy.Registry registry = new GrpcProxy.Registry(proxy);
        //Connect the proxy to the http server
        int port = 50051;
        Server httpServer = ServerBuilder.forPort(port)
                .fallbackHandlerRegistry(registry)
                .build().start();

        log.debug("Proxy Server started on port {}", port);

    }
}
