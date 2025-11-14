package org.example.filetransfer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.mgrpc.MessageServer;
import io.mgrpc.mqtt.MqttServerConduit;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.example.mqttutils.MqttUtils;

import java.io.*;

public class FileTransferService extends FileTransferGrpc.FileTransferImplBase {


    public static void main(String[] args) throws Exception {
        //Start embedded mqtt broker for test
        MqttUtils.EmbeddedBroker.start();

        //Get an mqtt connection to the broker and make a MessageServer from it
        //Services in the server will listen on topics prefixed with "tenant1/device1"
        final String brokerUrl = "tcp://localhost:1887";
        MqttAsyncClient client = MqttUtils.makeClient(brokerUrl);
        String serverTopic = "tenant1/device1";
        MessageServer server = new MessageServer(new MqttServerConduit(client, serverTopic));

        //Add the FileTransferService to the MessageServer
        if(args.length == 1) {
            server.addService(new FileTransferService(args[0]));
        } else {
            server.addService(new FileTransferService(null));
        }
        server.start();


    }

    private final String copyFilePath;

    public FileTransferService(String copyFilePath) {
        this.copyFilePath = copyFilePath;
    }

    @Override
    public void serverStreamFile(FileRequest request, StreamObserver<FileChunk> responseObserver) {

        //Stream a file chunk by chunk up to the client
        File file = new File(request.getFilePath());
        if(!file.exists()){
            responseObserver.onError(new FileNotFoundException(request.getFilePath()));
        }
        byte[] buffer = new byte[request.getChunkSize()];
        int bytesRead;
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
    }


    @Override
    public StreamObserver<FileChunk> clientStreamFile(StreamObserver<FileResult> responseObserver) {

        if(copyFilePath == null){
            System.out.println("No copy file path specified");
            responseObserver.onError(new FileNotFoundException("No file path specified"));
            return null;
        }
        File file = new File(copyFilePath);
        if(file.exists()){
           file.delete();
        }

        try {
            file.createNewFile();
            final FileOutputStream fos = new FileOutputStream(file);

            return new StreamObserver<FileChunk>() {
                @Override
                public void onNext(FileChunk value) {
                    try {
                        fos.write(value.getChunk().toByteArray());
                    } catch (IOException e) {
                        responseObserver.onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    FileResult result = FileResult.newBuilder().setOk(true).build();
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                }
            };
        } catch (Exception ex){
            responseObserver.onError(ex);
            return null;
        }
    }
}
