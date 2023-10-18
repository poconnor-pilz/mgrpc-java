
# Transport

Google:
how to implement a grpc transport site:stackoverflow.com

"In short, the answer is no. in Grpc.Core the native layer actually supports adding custom transports but doing so is a lot of work and requires expert knowledge of gRPC C core internals (and you'd need to recompile the native component from scratch) - so I wouldn't recommend going down this path. In grpc-dotnet this is currently also not possible (you need to use the default http2 transport)."
https://stackoverflow.com/questions/65804270/how-to-create-and-use-a-custom-transport-channel-for-grpc

There was some other answer saying that the transport is tied to the semantics of http/2 and is byte based not message based. So if your underlying protocol does not match this then don't use transport.


Also:

So transport is pluggable in two ways: 1) you can implement ManagedChannel
and Server directly or 2) you can use ManagedChannelImpl and ServerImpl as
utilities and then implement the transport that they expect. (1) basically
implements "all the hard parts of gRPC". (2) is still involved, but some
things like load balancing and service config would be handled for you. (2)
is typically what we mean by "implementing a transport".

https://groups.google.com/g/grpc-io/c/X6IVEKwrafo/m/VkfWwJXmBwAJ

But note that where ManagedChannel and things like ServerCall are in io.grpc classes like ManagedChannelImpl and 
ServerTransport are in io.grpc.internal so these are clearly not meant to be used (may be subject to change?)

Note that if we were to try the transport route the place to start would be to change the ClientTransportFactory passed to the
ManagedChannelImpl constructor.
Then that factory would need to create a new transport which in turn can create a new stream.
Then in the stream.flush() you would send the accumulated bytes to some topic. Then at that topic the inverse would send a response to some 
other topic. The ClientTransport.newStream gets  (MethodDescriptor<?, ?> method, Metadata headers, CallOptions callOptions) so this 
could be used for topics and to encode the headers etc.
ServerCallImpl shows how the stream is used. Note that Stream is not a java stream. It's a custom interface that looks easyish to implement.
The framework calls flush() is so the message could be sent over mqtt at that point.
Could make a messaging transport that in turn wraps a particular impl like mqtt but maybe just start with pure mqtt one.
The plugging in of different transports seems to start at ManagedChannelProvider.getDefaultRegistry()
This uses the java serviceloader mechanism to populate a registry with implementations of ManagedChannelProvider.class
See the comment on top of ManagedChannelProvider.class for how to plug it in.
Basically the user includes the jar that contains the impl of ManagedChannelProvider and it has a meta-inf that points to the
ManagedChannelProvider class which is then the default provider. After that you can do what you like but you could look at the 
Netty provider and copy that.
Then it should be possible to build it with forTarget("mgrpc://device1")
Should do this for the existing channel implementation anyway. 
To do transport then create an instance of ManagedChannelImpl but pass it your own ClientTransportFactory in its constructor

On the server side see the comment on ServerProvider class which works in a similar way.
The server provider only has a forPort(int port) method so we would need something else to set the root topic. (could use the port number as a string topic to start)
Then there is ServerImpl which we could re-use by passing our own impl of InternalServer to it.
InternalServer has a start(ServerListener listener)
and ServerListener has a
ServerTransportListener transportCreated(ServerTransport transport);
See also InProcessTransport and InProcessStream
Could start by just copying all the InProcess classes and modifying them. 

Could debug the in process server flow from channel to server to see a simple implementation of all of this.


The value level up is to use the Channel interface

## Protcol buffers RPC
The value level up from that again is to use protocol buffer RPC. You just modify the protocol buffer compiler to generate your own proxy and stub
See the two links in the answer to this question
https://stackoverflow.com/questions/59616929/how-can-i-write-my-own-rpc-implementation-for-protocol-buffers-utilizing-zeromq

This is the main documentation
https://developers.google.com/protocol-buffers/docs/proto#services

See the javadoc here
https://developers.google.com/protocol-buffers/docs/reference/java
It says:
Starting with version 2.3.0, RPC implementations should not try to build on this, but should instead provide code generator plugins which generate code specific to the particular RPC implementation. This way the generated code can be more appropriate for the implementation in use and can avoid unnecessary layers of indirection.

So this contradicts the main documentation. But it looks like the protobuf services syntax is independent of grpc and you just plug into the compiler at a high level and generate your code.


## Channel, ClientCall, ServerCall

This seems to be the middle level. The basic architecture is roughly:

Request->ClientCall.sendMessage()->Transport->ServerCall.Listener.onMessage()
ClientCall.Listener.onMessage()<-Transport<-ServerCall.sendMessage()<-Response

There are two points where listeners and calls are wired together:

ClientCall::start(Listener<RespT> responseListener, Metadata headers)
and
ServerCall.Listener<RequestT> ServerCall::startCall(ServerCall<RequestT, ResponseT> call, Metadata headers);

And also there's code where StreamObserver is converted to the sendMessage/onMessage stuff above

Where at the far right is the method implementation got from the ServiceImpl

There's a bit more to it than that but can be worked out by debugging this:


        //Make an InputStream from a HelloRequest
        final HelloRequest hr = HelloRequest.newBuilder().setName("joe").build();
        InputStream stream = new ByteArrayInputStream(hr.toByteArray());

        final ExampleHelloServiceImpl exampleHelloService = new ExampleHelloServiceImpl();
        final ServerServiceDefinition serverServiceDefinition = exampleHelloService.bindService();
        final ServerMethodDefinition<?, ?> sayHello = serverServiceDefinition.getMethod("helloworld.ExampleHelloService/SayHello");
        final Object theHelloRequest = sayHello.getMethodDescriptor().parseRequest(stream);
        final ServerCallHandler serverCallHandler = sayHello.getServerCallHandler();
        ServerCall serverCall = new ServerCallTry(sayHello.getMethodDescriptor());
        final ServerCall.Listener listener = serverCallHandler.startCall(serverCall, new Metadata());
        listener.onMessage(theHelloRequest);
        //notify this is the end of the client stream after which the onMessage() will go through
        listener.onHalfClose();

Where ServerCallTry is just an empty implementation of ServerCall but whose getMethodDescriptor returns the method descriptor passed in ctr


The value alternative is to use the channel interface and then get the benefit of generated blocking stubs etc. This means that we do not have to deal with bytes etc. A channel just creates a client call given a method descriptor. ClientCall is here:
https://grpc.github.io/grpc-java/javadoc/io/grpc/ClientCall.html
There is an example at the top of it that shows how it works
When the client has no more requests to send on a client stream it calls halfClose(). So we would send completed here. In the case of a unary call we would send nothing.
To debug the in channel version see C:\dev\gitpublic\grpc-java\examples\src\test\java\io\grpc\examples\helloworld\SimpleInprocessTest.java
Also see:
io.grpc.internal.ClientCallImpl
This is a class that may have some helpers on how to implement a client call

Note that errors are handled in ClientCall.onClose() by sending a bad status.

TODO: Get an inprocess call going by using a manual registry? What is the registry? Debug into the InProcessChannelBuilder to see how that works.



### Flow control/Backpressure



The only other thing that is different from the mqtt implementation is flow control. Specifically the client does:
// Notify gRPC to receive one additional response.
call.request(1);

Its not clear if the grpc client will buffer the message until it gets a request(n) or if it actually sends the request(n) on to the server. It probably sends it on.
We are unlikely to send it on over mqtt as this would be expensive in terms of the number of messages back and forth for each value. This is not how mqtt works anyway and so mqtt solutions don't have this.
In fact to do this it would probably be more efficient for the client to just pull i.e. to keep making requests on some stream id (well except that then there would be one value sent per request wheras with request(10) 10 values can be sent for one request so maybe that is not too bad if the n is big enough.)
One way that might work is to use the broker as a buffer. If we use a shared client then all messages come off one pipe which is why each subscription blocks. But if we use a client per grpc client/stub then the subscription could just block on a lock until the client does request(n) and then it could read n messages and send them on.
This would be expensive as clients are slow to connect and each client has a bit of memory and starts a few threads. The clients could be pooled but if things got busy then they might all end up blocking but this could be tested.

A last way would be to have a pool of mqtt connnections and to only have one call ongoing on one connection at any time. The subscription for this call could then 'block' until the client requests more messages. This hopefully would cause the broker to buffer the messages until the subscription unblocks. But this would have to be tested to verify that if the sub blocks for a long time and in the meantime there are a lot of publishes to that topic that the broker doesn't end up dropping messages or failing a publish. It is unusual in mqtt to have multiple connections. But the concept of many tcp connections to a server is not unusual - db connection pool, http client can have many connections to a server (and create and drop connections frequently) etc. One connection per method might not be ridiculous.

Note: We could also just have a fixed number of 'channels' for the server. These are just subtopics /ch1,/ch2 etc. Client could query server for how many channels it supports and then round robin the requests to those. Server would have a separate mqtt client for each and so would client. Then maybe don't have topics for methods. Just look at the method descriptor in the message header. But this would mean that you could not have broker policies per method.

In the end of the day though if we use the broker to buffer things then this solution would fail if we have a local broker like mosquitto. We will just be delegating the memory usage to mosquitto and the machine will eventually run out of memory. The only real way to do backpressure is to stop the data getting in at source.

See C:\dev\gitpublic\grpc-java\examples\src\main\java\io\grpc\examples\manualflowcontrol\ManualFlowControlClient.java
It looks from the comments in this that if you don't do manual flow control then there is no backpressure? Also it appears to depend on extending a special ClientResponseObserver class.

Maybe if we have to do flow control or backpressure it might be better to use just request response where the response includes a batch of repeated fields. In the case of watches it will be ok anyway as we do flow control in the form of batching every 50ms max. If the client can't handle this then we could do it in the form of batching every 100ms. 
On the client side we are in the cloud so the client should be able to allocate plenty of memory and queue things if there are bursts. If there is just too much constant throughput then the solution to this is something like opc where the client tells the server the max update rate. This is better than backpressure becuase backpressure is only good for bursts. If we have a system with backpressure where there is constant high throughput then eventually the server will have to throw stuff away.
For the server side then backpressure could be good to control a client stream. But we may not have many instances of client streams and there may be other ways of handling it like a bidi stream where the client only sends the value thing when it gets an 'ack' although this is very similar to backpressure.

It may be possible to do a mix. For request response just use one connection for all methods. Then each method has a small bounded queue of requests. If that queue fills up the method fails the request. Then for streams use a separate connection pool and each stream just handles things directly on the subscribe thread, blocking it until it is ready for the value message, thereby using the broker as a buffer. Or else we could just implement backpressure for streams with the client telling the server to send on n messages. This is probably the best solution. It is not wasteful for request response (no request(n) messages being sent) and it also works for streams.

Maybe for the start implementation just have a single subscription with each method having a bounded queue whether for requests or streams. If the queue fills then the request or stream fails. Then start to look at other ideas above later (starting with backpressure/request(n) for streams)
Each method doesn't even have to have a bounded queue. There can just be a thread pool with each method running on one of the threads. Then just size the thread pool i.e. the same as the servlet model. This effectively becomes a kind of bounded queue anyway except it is across all methods so if one method is slow and gets called a lot then it may hog the pool though. The other thing about the bounded queue per method is that it guarantees that a particular method doesn't have to be thread safe which is important because the service of that method is a singleton. So the summary is. Each method has a bounded queue that is served by a single thread that may have come from a thread pool. When the queue is empty then the thread is returned to the pool.
Actually: It looks like server methods are expected to be thread-safe/re-entrant (https://grpc.io/blog/optimizing-grpc-part-2/). If you run a test you can see that the methods are fully re-entrant. So it looks like we should just have a thread pool/executor and let the server decide whether or not to be parallel. If it doesn't want to be parallel then it can just put the synchronised keyword on the method as in the linked example. The only difference here with a queue is that it now hogs one of the threads in the pool (but loom will fix this later).

Summary: Methods are executed on a thread pool like classic grpc or like servlets. When the thread pool is fully busy then client requests will be blocked or time out. If a service implementer wants so serialize access to a method then they should mark it synchronized (which will serialize access to the whole object). Note that for a particular call to a method the messages in the stream for it are send sequentially, i.e. that stream's onNext will not be called until that stream's previous onNext is finished. So the method is re-entrant but the call is not.  We may introduce flow control later where the server or the client can explicitly request a certain number of messages to be sent. If this is the case then we will need a new kind of message (a flow control message) which will only make sense where there are many messages in a stream. It is true however that even though this matches classic grpc closely it does not make use of the potential of the broker to queue requests which is a big advantage of having a broker as part of the system. With a classic broker the server can just process methods in its own time (even though mqtt is not really a classic broker) with memory usage being pushed back to the cloud (although with flow control memory can be pushed back to the cloud client if it makes its own queues etc). However by using the classic grpc approach it means that the server and client can also be deployed over https and should still work because they have no dependency on broker buffering.


## Older Transport Notes
We could also aim so that the stub code behaves the same way although this will be harder as the stub is generated code. It's not some interface that we can implement. It's a pity that the grpc stub does not implement the same interface as the service. Then we could drop in another stub implementation. It's ok for us to have our own stub as long as we say that we only have to support two situations: Local in process calls and mqtt calls. If we also want to support https calls then we might need to support the full grpc stub but backed by something else. It may be possible to do this if we can support the channel interface. It looks like this is possible. Put a bp in the call to blockingStub.sayHello and watch how it is constructed. You have to just extend Channel and then implement the methods. The client will call newCall which passes a full method descriptor (service name, method name, method type e.g. UNARY). Then you return something that implements ClientCall. Then something will call ClientCall.start which takes a listener and that is what you pass the responses to. See DelayedListener and DelayedClientCall. Can put bps in these. So it is definitely possible. 
See: 
https://github.com/grpc/grpc-java/compare/master...jhump:jh/introduce-in-process-channel
https://github.com/grpc/grpc-java/issues/518

Transport is at too low a level, dealing with byte streams like sockets. Also deals with flow control and backpressur between client and server but this doesn't apply  in mqtt where client has no direct connection to server. Backpressure etc is handled by the broker.

Also may be able to use some of the server infrastructure. Run the helloworld example and put bp in the GreeterImpl.sayHello. Up the stack there is a methodDescriptor etc.
A GreeterImplBase is a BindableService (see in the HelloWorldServer.start it is used by addService). A BindableService has  a  ServerServiceDefinition on which you can call getMethod() and this returns a callble method. (Put a bp in sayHello and up the stack you will see it getting called). The problem is how do you reify the types that are being passed to it. It looks like it might not be needed as the method has the classes of the types when constructed (again see GreeterGrpc.java bindService() it makes the types there. So they may be fine. But where do we call with protobuf?. There is a MethodDescriptor.parseRequest and parseResponse and these may work fine.
It looks like this is possible see HelloWorldServer.java
    final HelloRequest hr = HelloRequest.newBuilder().setName("test1").build();
    InputStream strm = new ByteArrayInputStream(hr.toByteArray());
    final List<ServerServiceDefinition> services = server.getServices();
    final ServerServiceDefinition serverServiceDefinition = services.get(0);
    final ServerMethodDefinition<?, ?> sayHello = serverServiceDefinition.getMethod("helloworld.Greeter/SayHello");
    final Object theHelloRequest = sayHello.getMethodDescriptor().parseRequest(strm);
    final ServerCallHandler<?, ?> serverCallHandler = sayHello.getServerCallHandler();

i.e. we have the HelloRequest parsed to theHelloRequest. But the handler is not the direct handler in GreeterImpl methodHandlers. Its a ServerCallHandler. This does contain the method handler but this is private and you have to go through the ServerCallHandler.start which gets passed a stream at some point. Maybe we could make a stream of the mqtt bytes for the param. But also its a unary call etc. So possibly TODO investigate later (maybe investigate the in process version to see what that does by running HelloWorldServerTest. It probably doesn't use streams? 
**Also look at the git code above for the InprocessChannel stuff. This seems to call the server directly. Or even try to just make an inprocess call using the inprocess transport as below but use it generically?
But for the moment this is not important because we can just generate the skeleton which much simpler than the generated code for GreeterImpl because it doesn't have to deal with streams and half close etc (broker handles all that). So it's probably better to use the simpler skeleton code than making it more complicated just to be able to use the default generated code.

The more important thing is to try to mimic the client (blocking stub etc) because the client is the Interface. And we want to write our clients against a single interface. The server is just an implementation and ok there might be a bit of re-use of some but it's not about interoperability which is the important thing. 

To run a service in process (see SimpleInprocessTest) as documented in InProcessServerBuilder:

    public static void main(String[] args) throws IOException {
        String uniqueName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(uniqueName)
                .directExecutor() 
                .addService(new HelloWorldServer.GreeterImpl())
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(uniqueName)
                .directExecutor()
                .build();
        GreeterGrpc.GreeterBlockingStub blockingStub = GreeterGrpc.newBlockingStub(channel);
        HelloReply reply =
                blockingStub.sayHello(HelloRequest.newBuilder().setName( "test name").build());
        assertEquals("Hello test name", reply.getMessage());
        channel.shutdown();
        server.shutdown();

Although if directExecutor() above means that calls on the channel to any service will be serialised it might be better to have a threaded executor as that will be more efficient and also will mimic how the mqtt channel will behave.

