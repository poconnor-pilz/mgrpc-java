package io.mgrpc;

public interface RpcMessageHandler {
    void onRpcMessage(RpcMessage message);
}
