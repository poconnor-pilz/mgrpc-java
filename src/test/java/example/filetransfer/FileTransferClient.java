package example.filetransfer;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.stub.StreamObserver;
import io.mgrpc.*;
import io.mgrpc.mqtt.MqttChannelConduit;
import io.mgrpc.mqtt.MqttUtils;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class FileTransferClient {

    public static void main(String[] args) throws Exception {

        //This client expects a file path to be passed as the single argument e.g. C:\tmp\MyFile.txt
        //It will call a File Transfer server asking it to stream that file to the client
        //The client read take the stream and copy it to a new file called C:\tmp\CopyOf-MyFile.txt

        if (args.length != 1) {
            System.out.println("Usage: FileTransferClient <filePath>");
            return;
        }
        String filePath = args[0];
        File file = new File(filePath);
        File copyFile = new File(file.getParentFile(), "CopyOf-" + file.getName());
        if (copyFile.exists()) {
            System.out.println("File " + copyFile.getAbsolutePath() + " already exists");
            return;
        }

        //Make a grpc channel on topic "filetransfer" on the local broker
        MqttAsyncClient clientMqtt = MqttUtils.makeClient();
        MessageChannel msgChannel = new MessageChannel(new MqttChannelConduit(clientMqtt));
        String serverTopic = "tenant1/device1";
        Channel channel = TopicInterceptor.intercept(msgChannel, serverTopic);

        //Code from here on down is pure gRPC (no mqtt)
        FileTransferGrpc.FileTransferBlockingStub stub = FileTransferGrpc.newBlockingStub(channel);

        int chunkSize = 1024 * 20; //Max payload size for AWS IoT hub is 128kb.
        byte[] buffer = new byte[chunkSize];
        int bytesRead;


        try (FileOutputStream fos = new FileOutputStream(copyFile)) {
            FileRequest request = FileRequest.newBuilder()
                    .setFilePath(filePath)
                    .setChunkSize(chunkSize)
                    .build();

            final Iterator<FileChunk> fileChunkIterator = stub.serverStreamFile(request);
            while (fileChunkIterator.hasNext()) {
                FileChunk fileChunk = fileChunkIterator.next();
                fos.write(fileChunk.getChunk().toByteArray());
            }
            fos.flush();

            System.out.println("File " + file.getAbsolutePath() + " has been copied to " + copyFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("An error occurred while writing the file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            msgChannel.close();
            clientMqtt.disconnect();
            clientMqtt.close();
        }
    }

}
