
# mgRPC-Java 


This repository implements gRPC over MQTT for java clients. It also contains a proxy that can support gRPC clients written in any language.


## Building mgRPC-Java

Building requires at least JDK 8


To build, run:
```
$ ./gradlew build
```

To install the artifacts to your Maven local repository for use in your own
project, run:
```
$ ./gradlew publishToMavenLocal
```


## Getting Started

Have a look at the [examples](examples/readme.md)


## Key Attributes
- Maintains message order and detects duplicates even with underlying brokers that do not have these features.
- Supports errors, cancellation and timeouts and authentication.
- Can communicate with multiple servers over a single channel by specifying the root topic for each server.
- Supports a proxy/http server to which any grpc client (in any language) can connect.
- Topic structure makes it convenient to implement security via broker policies.
- Will only send one mqtt message for a client that sends one request only (i.e. no extra header message).
- If the Mqtt connection specifies an LWT and a client is disconnected, all calls related to that client will be cancelled via the gRPC cancel mechanism.


## Topics

mgRPC publishes and subscribes to broker topics. The general topic structure is:
```
{serverTopic}/i|o/svc/{channelId}/{slashedFullMethodName}
```
For example, the client sends requests to:
```
tenant1/device1/i/svc/helloworld/ExampleHelloService/SayHello
```
The server sends replies to:
```
tenant1/device1/o/svc/l2zb6li45zy7ilso/helloworld/ExampleHelloService/SayHello
```
  where,
  - serverTopic = tenant1/device1    
  - channelId = l2zb6li45zy7ilso
  - gRPC full method name = helloworld.ExampleHelloService/SayHello

The server topic is specified when constructing the server e.g. to specify a server topic of "tenant1/device1":
```
MessageServer server = new MessageServer(new MqttServerConduit(mqttConnection, "tenant1/device1"));
```
The channelId is usually created automatically by the ```MessageChannel``` constructor but it is possible to override it. 

One channel can communicate with many servers, meaning the channel will have multiple subscriptions to e.g.
```
tenant1/device1/o/svc/l2zb6li45zy7ilso/#
tenant1/device2/o/svc/l2zb6li45zy7ilso/#
```
(Subscriptions for these topics are managed via the MqttTopicConduitManager)

## Connection Status and LWT

### Server Status
Server status is used so that mgRPC can detect when a server is shut down or disconnected and send error messages to associated channels. Server status messages are published to:
```
  {serverTopic}/o/sys/status
```
The server will publish a ConnectionStatus message with connected=true to this topic when it starts up or when a client sends it a message on:
```
  {serverTopic}/i/sys/status/prompt
```
The server will publish a ConnectionStatus message with connected=false message to {serverTopic}/o/sys/status when it shuts down normally.

### Server LWT
The server should be constructed with an MQTT connection that has an LWT topic set to ```{serverTopic}/i/sys/status/prompt``` and an LWT message consisting of a ConnectionStatus message with connected=false. This will ensure that the channel is notified if the server is terminated or disconnected abnormally.

### Channel Status
Channel status is used so that mgRPC can detect when a channel/client is shut down or disconnected. mgRPC will inform the server which can send errors to any existing services that were associated with the channel and terminate their streams. Because one channel can communicate with many servers it is not possible to base the channel status topic on a server topic. For this reason the channel status topic must be specified by the client. This must be done during construction of the MessageChannel:
```
MessageChannel channel = new MessageChannel(new MqttChannelConduit(mqttConnection, "tenant1/channelstatus"));
```
and also during construction of the server
```
MessageServer server = new MessageServer(new MqttServerConduit(mqttConnection, "tenant1/device1", "tenant1/channelstatus"));
```
If the application involves multiple tenants that are isolated by broker policy then it is important to include a representation of the tenant in the channel status topic (e.g. "tenant1/..." in the example above). This is so that one tenant cannot listen for status messages of another and discover the other's channel id's.  

### Channel LWT
The channel should be constructed with an MQTT connection that has an LWT topic set to the channel status topic and an LWT message consisting of a ConnectionStatus message with connected=false. This will ensure that the server is notified if the server is terminated or disconnected abnormally.




