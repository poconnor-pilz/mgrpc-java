# mgrpc-java examples

These examples demonstrate how to use mgRPC-java, a library for running gRPC over MQTT
To run the examples first make sure to [build and publish](../readme.md) mgrpc-java.
Then build the examples using
```
$ ./gradlew build
```

## Greeter Examples

The greeter examples show a simple example of each kind of service method as described [here](https://grpc.io/docs/what-is-grpc/core-concepts/)

Note that the greeter examples will start a local embedded broker that listens on tcp://localhost:1887. You can change the broker url by modifying the contents of src/main/resources/broker.properties

You can run the greeter examples directly from gradle.


Start the greeter service
```
$ ./gradlew run -Pargs="GreeterService"
```

Unary RPC example
```
$ ./gradlew run -Pargs="Unary"
```
Server streaming RPC example
```
$ ./gradlew run -Pargs="ServerStreaming"
```
Client streaming RPC example
```
$ ./gradlew run -Pargs="ClientStreaming"
```
Bidirectional streaming RPC example
```
$ ./gradlew run -Pargs="BidirectionalStreaming"
```

