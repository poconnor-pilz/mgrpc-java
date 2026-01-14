package io.mgrpc;

public class ConnStatus {

    /**
     * Make a protocol buffer containing a connection status
     * @param status The status of the connection
     * @param channelId The channelId if this status is for a channel. Otherwise null.
     * @return The protocol buffer containing the encoded connection status
     */
    public static byte[] makeConnectionStatusProto(boolean status, String channelId){
        return ConnectionStatus.newBuilder().setConnected(status).build().toByteArray();
    }
}
