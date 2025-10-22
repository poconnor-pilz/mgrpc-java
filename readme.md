
# mgrpc-java 


This repository supports transport of gRPC over MQTT for java clients. It also contains a proxy that can support 
gRPC clients written in any language. 

Key attributes:
- Maintains message order and detects duplicates even with underlying brokers that do not have these features.
- Supports errors, cancellation and timeouts and authentication.
- Can communicate with multiple servers over a single channel by specifying the root topic for each server.
- Supports a proxy/http server to which any grpc client (in any language) can connect.
- Topic structure makes it convenient to implement security via broker policies.
- Will only send one mqtt message for a client that sends one request only (i.e. no extra header message).
- If the Mqtt connection specifies an LWT then if a client is disconnected, all calls will be cancelled via the gRPC
    cancel mechanism.



