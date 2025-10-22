package example.filetransfer;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageChannel;
import io.mgrpc.StreamWaiter;
import io.mgrpc.TopicInterceptor;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

import java.io.*;
import java.util.Iterator;

public class LatestPtmClient {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage: LatestPtmClient <filePath>");
            return;
        }
        String filePath = args[0];
        File file = new File(filePath);

        //Make a grpc channel on topic "filetransfer" on the local broker
        MqttAsyncClient clientMqtt = MqttUtils.makeClient();
        MessageChannel msgChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        String serverTopic = "tenant1/device1";
        Channel channel = TopicInterceptor.intercept(msgChannel, serverTopic);

        //Code from here on down is pure gRPC (no mqtt)
        FileTransferGrpc.FileTransferStub stub = FileTransferGrpc.newStub(channel);
        StreamWaiter<FileResult> waiter = new StreamWaiter<>(100000);
        final StreamObserver<FileChunk> responseObserver = stub.clientStreamFile(waiter);

        int chunkSize = 1024 * 20; //Max payload size for AWS IoT hub is 128kb.
        byte[] buffer = new byte[chunkSize];
        int bytesRead;
        if(!file.exists()){
            responseObserver.onError(new FileNotFoundException(filePath));
            return;
        }
        try(FileInputStream fStream = new FileInputStream(file)){
            while ((bytesRead = fStream.read(buffer)) != -1) {
                FileChunk chunk = FileChunk.newBuilder()
                        .setChunk(ByteString.copyFrom(buffer, 0, bytesRead))
                        .build();
                responseObserver.onNext(chunk);
            }
            responseObserver.onCompleted();
        } catch (Exception ex){
            responseObserver.onError(ex);
        }

        final FileResult single = waiter.getSingle();
        System.out.println("Result: " + single.getOk());

        msgChannel.close();
        clientMqtt.disconnect();
        clientMqtt.close();
    }

}
