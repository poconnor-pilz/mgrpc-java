
## General 


Key attributes:
Tunnels grpc over mqtt.
Can talk to multiple servers over one channel by specifying the root topic for that server.
If a client or server is disconnected, all calls will be cancelled.
Orders messages (some cloud brokers do not guarantee ordering)
If direct mqtt connection is used in java client then it will only send one mqtt message for a single request
Supports a proxy/http server to which any grpc client (in any language) can connect.
Topic structure makes it possible to implement security via broker policies but can also use auth tokens. 

