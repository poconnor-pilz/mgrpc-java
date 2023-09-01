package io.mgrpc;

public class ConnStatus {

    /**
     * Make a protocol buffer containing a connection status
     * @param status The status of the connection
     * @return The protocol buffer containing the encoded connection status
     */
    public static byte[] makeConnectionStatusProto(boolean status){
        return ConnectionStatus.newBuilder().setConnected(status).build().toByteArray();
    }
}
