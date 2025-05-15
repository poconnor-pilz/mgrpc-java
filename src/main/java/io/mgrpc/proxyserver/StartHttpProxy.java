package io.mgrpc.proxyserver;

import io.grpc.*;
import io.mgrpc.GrpcProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/**
 * Run this to test and compare normal http proxy
 */
public class StartHttpProxy {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) throws Exception {

        int baseServerPort = 50050;
        Server httpServer = ServerBuilder.forPort(baseServerPort)
                .addService(new HelloServiceForTest())
                .build().start();


        String target = "localhost:" + baseServerPort;
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                .build();
        log.info("Proxy will connect to " + target);
        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(channel);
        int port = 50051;
        Server server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .fallbackHandlerRegistry(new GrpcProxy.Registry(proxy))
                .build()
                .start();
        log.info("Proxy started, listening on " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                server.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            server.shutdownNow();
            channel.shutdownNow();
        }));
        server.awaitTermination();
        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Channel didn't shut down promptly");
        }




    }
}
