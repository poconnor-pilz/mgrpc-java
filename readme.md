
## General 

See also transport.md for stuff about the transport

We should try for smart endpoints, dumb pipes. So let the final user client code deal with exceptions and lost connections and re-connecting etc.
Responsibility of proxy and stub classes is just to map method names and protobufs to actual methods and typed messages.

Why not use a reverse proxy? For the same reason that we don't use a VPN. Some customers will not want any inbound port open on their network.
See also this article: 
https://www.automation.com/en-us/articles/2016-2/making-opc-ua-secure-for-the-industrial-iot
(AWS also have a secure tunnel which works by having a proxy either side where the factory side only makes an outbound connection.
The client then connects to a proxy instead of the device. But there is one tunnel and proxy per device and the tunnel only stays open for 
12 hours max. It looks like it is primarily for use for troubleshooting particular devices in a single session. Not for ongoing cloud stuff.)
## Ordering
We should probably support a counter based id for this. It can be per stream as the stream will be distinguished by topid or if there is a shared topic then by something like a watchId or requestId. For request response ('unary' in grpc terms) can just set it to zero. Ordering will have to be managed as for services it will be important. For example a service that streams a project down to a device. So ordering should be built in. Look at the google mqtt javascript client code for this.
For google iot the suggestion for ordering is here:
https://web.archive.org/web/20190629192452/https://cloud.google.com/pubsub/docs/ordering
But this just buffers messages until a timeout and then sorts those and processes them. They could have done better than this by only buffering as soon as a messsage that doesn't have a counter of previous+1 arrives. But maybe this is because they can't guarantee that all messages are delivered (which mqtt does).
Note that the latest version of this doc states that you can specify to the pubsub system that your message has an ordering key and it will order the messages delivered to you. But this version of the doc basically says to get all the messages in a stream (or up to a timeout here) and then order them all before dealing with them.
Is there a way of just doing this sorting only if the value message does not have the last message id+1? Then start buffering messages until you get the ones you are missing. But what if in the meantime there are more messages coming in out of order. When do you stop? Just make the buffer and keep sorting it until all the messages are one plus the last. Then send on that buffer's worth. Do this in a blocking function so you don't receive new messages while sorting. Maybe in the sending on bit have a blocking queue that you send the ordered messabes to and a separate thread to service that in case the client is slow. Also overall for the sorting have a timeout so that if you don't get ordered messages within a certain time you just fail. Also throw away duplicates. Overall this should not happen frequently and the out of order message should appear very soon as it's just a network delay. If qos is 1 then you are guaranteed to get it at least once. There should be no waiting for ages for it. The fact that you got message 3 before 2 means that 2 was sent already so it's not like you are waiting for the server to send it. See TryOrdering.java for this.

## Cancellation
How do we implement stream cancellation? (Note that in ordinary mqtt (e.g. the watch code) we would not have supported something as sophisticated as this at all. Instead we explcitly registered and deregistered watches and this works fine. It looks like earlier versions of grpc did not support cancellation either (although a hack was for the client stream to send an errror) )

On cancellation MqttChannel.MqttCall.cancel() should get called.
This does get called if we send an error in the client stream
It doesn't get called if we do a blocking request with timeout.
To see why make an in process call with timeout. Then put a bp in CallOptions.getDeadline
You will see that ClientCallImpl.start calls startDeadlineTimer.
Somehow this will eventually throw an exception that does the call cancel
(use ChannelWrapper with bp in cancel to see this)
It is a scheduled executor and the clientCall will also cancel it in case of a higher level cancel.

But the summary is that you have to look in the calloptions for a deadline and then start something in the call start that calls back to cancel the call. But should also look at the current context (get context from thread local originally) like the ClientCall and see if it also has a deadline.
When the call is cancelled it should clean up after itself and send a cancel down to the server
This could be sent as an error/completion with deadline exceeded. Then even though the server call may already be  half closed it should be still possible to send a cancel to it.
To send a cancel, send a completed message with Status==CANCELLED

The following shows how the service code should check for cancellation
https://grpc.io/blog/deadlines/
`
if (Context.current().isCancelled()) {
    responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
    return;
}

OR can do

Context.current().addListener(listener, directExecutor())

where listener is a Context.CancellationListener 
`
How ClientCallImpl does it:

In its constructor
    this.context = Context.current(); //It probably needs this in case it is part of a relay/forwarding ?
    this.callOptions = callOptions;
    this.deadlineCancellationExecutor = deadlineCancellationExecutor;//This will be a ScheduledExecutorService

private Deadline effectiveDeadline() {
    // Call options and context are immutable, so we don't need to cache the deadline.
    return min(callOptions.getDeadline(), context.getDeadline());
}

Then at the end of its startInternal()
It calls startDeadlineTimer which simply creates a runnable of type DeadLineTimer which just 
calls back stream cancel when it runs (after the deadine)
But this also returns a future which it if something does a high level cancel then it 
does a cancel on that (so taht the scheduledexec is cancelled.)
It does this in removeContextListenerAndCancelDeadlineFuture

Also look at usages of ServerCallImpl.cancelled.

Cancellation:
It looks like for server cancellation because of timeout that the channel doesn't send a cancel on timeout. 
Instead it sends the deadline in the header of the call and the server sets its own timer
See ServerImpl.java  Context.CancellableContext createContext


To explicitly cancel a call from a test it looks like the only thing that works is to run a http server
(not inproc) and then make a
class CancelableObserver implements ClientResponseObserver<HelloRequest, HelloReply>{
    private ClientCallStreamObserver requestStream;
    @Override
    public void beforeStart(ClientCallStreamObserver reqStream) {requestStream = reqStream;}
    public void cancel(String message){requestStream.cancel(message, null);
....

with OnNext etc. Then on the client side, pass this as the return stream to a method. 
Then you can call cancel on this.
You are also supposed to be able to do:
cancellableContext = Context.current().withCancellation();
cancellableContext.cancel(null);
But in this case the server never gets the cancel. (because the context is wrong? Tried setting it even in the call )

Also look at ManagedChannel. That has shutdown methods that are supposed to send cancels. Would need to test this.

Because the cancel has nothing to do with the client stream or input request then it looks like we will need
an MgType.CANCEL and send that. Then somehow the server has to put that in the context for that call.

The server call should not send back anything to the client when it is cancelled. If it is cancelled from the client then the client will know about the cancel anyway. In that case the channel should send an onError to any streams it has with Status.CANCELLED (this seems to be what grpc classic does).  If it is cancelled because the server is shut down then the channel should detect that through the LWT and send cancels to client streams.

TODO: 
- In MqttClientCall listen to context for cancels and call cancel()
TODO: tests
- Test parallelisim

## Watch Batching
Batching is something that comes up as a specific optimisation for a cloud visu server use case and is probably not a general concept at all. 
In the cloud visu server we want to be able to create a number of watches and then as the values come in batch them together and send them in one message. So if values come in for watch1 and watch2 within a 50ms interval then the visu server will send those to us in one message. We further want to send them in one mqtt message to the cloud visu server.
The way to model this is to say that watch1 and watch2 should be part of one watch stream with a streamId
When the client creates a watch it specifies a watchId and a streamId.
Any watch value that comes in streamId is put into a queue for that stream.
This queue is cleared every 50ms.
Then the client will make a separate gRPC call listenForWatchStream(streamId) which returns a stream of watch values
If the queue has values (e.g. watch1 values and watch2 values) then these values are put into a repeated field and sent on via the server stream
i.e. the server maintains named streams which have nothing to do with topics or grpc (maybe it maintains them in a hashmap)
Then any client can just use the normal grpc to listen to the named stream by making a grpc request with the streamId as a parameter.
The desktop client will end up behaving in exactly the same way.
If two clients (two cloudvisuserver pods) make a request for the same stream then two mqtt messages will be sent, one for each because each has a separate replyTo.
This differs from the iot implementation where everything is batched on one topic.
But if we really want to suppport that we could using pub sub except that it would be a different method to the desktop client.
That is justified because the cloudvisuserver is a very special use case.
For most tooling though this batching mechanism will be more than efficient.
To really support this though we should consider the opc pub sub over mqtt spec and possibly implement it.
We could then use gRPC methods to configure what is published.
If those 'groups' that are published have as their id the hash of their content then we could guaranteee on the device that it never publishes a group twice.
To manage the lifetime of groups there would probably need to be a WatchManager service on the cloud that detects when clients have died and deletes watches.
It would also re-create watches when a device disconnects and re-connects etc.

## pub sub
Note that the main difference request response and events is that for events there has to be some kind of topic. A single server can efficiently send events to many clients  by just keeping a list of them and sending the event to each client channel when something happens. pub sub is an optimisation of this where firstly the server does not have to manage the clients. This means the server can be simpler but also the server will use less processor and bandwidth as it only has to have a single stream to the broker no matter now many clients of that stream. It just publishes to a topic. In theory another optimisation is that many servers could publish to one topic but this is not used much for IoT. Queueing is a separate issue and mqtt does not provide it. For IoT we don't really need the client managment part. We could easily do it ourselves if the clients could connect directly to the server. But they can't so the broker is a way of getting past the firewall and also it takes load and bandwidth off the device (see earlier). The problem for us is that if the device managed the clients itself then grpc could work perfectly with this. But now we have to find some way of making grpc work without much changes but still only having one stream to the broker.
If there is a use case that requires pub sub then we will assume that this will only work when there is a broker. Therefore this kind of service will be cloud only. 99% of tooling use cases will not need this. When there is a broker then we can just make a subscribe method that wraps the broker subscribe but takes a topic as the request and a StreamObserver. subscribe(String topic, StreamObserver<V> responseObserver)
You could argue that pubsub or mqtt is a more consistent model. The topic is first class and exposed to the user. So the same model covers both publish and subscribe. In both cases you specify a topic. But for grpc when you make a request you don't specify a topic. You specify a method. Then if you want to subscribe you have to have a subscribe method. But you could say that grpc is also a general model. You have a method that has two streams. The output stream can be used for subscription. You just have to specify the topic in the single message that you send to the input stream.
This would work even without a special subscribe method. We could just have a listenToWatch(watchId) method. It will work. But if many clients listen to the same watch then there will be many underlying mqtt subscriptions and duplicate messages. So the special subscribe method is just an optimisation. The client has to use normal grpc to call a createWatch method. The createWatch method will just return ok or fail (or maybe a watchId). Then the client has to subscribe to the watch topic to actually get the values. But the subscribe method looks like just a normal grpc method as far as the client is concerned. It takes a protobuf etc and it returns a stream of messages (which happen to have no end). It could be backed by any technology, an mqtt broker, an amqp broker or just something that is implemented internally in some server.
We could also consider just writing the service normally where it does not call mqtt publish. It is unaware of a broker like any normal grpc service and can run without a broker. But we provide some facility on the client side for mapping that to a topic. That manager on the client side would make sure to only call the grpc service once and then when other clients ask for the same topic or watch it would distribute that stream to them. So it is a fanout service that runs on the client side. If we did do this we could use options (annotations) in the IDL to mark a service as being pubsub where the request message must contain a Topic topic=1 and the infrastructure will decode that and map results to that topic. Then if a new client subscribes to the same topic the infrastructure just re-uses the same subscription and maps it out. Could do reference counting as well like the watch api.

Code to get options from a method descriptor (need to convert it to a different class of method descriptor)
                final ProtoMethodDescriptorSupplier schemaDescriptor =  (ProtoMethodDescriptorSupplier)serverMethodDefinition.getMethodDescriptor().getSchemaDescriptor();
                final DescriptorProtos.MethodOptions options = schemaDescriptor.getMethodDescriptor().getOptions()

Or can put a Pubsub object in the request message called pubsub. If it is not null the the server sends the response stream to the PubSub.topic. The server just sends a completed to the requester. The server listens as normal for a completed and cleans up the service when finished. The service itself has to manage lifecyle of whatever it is producing. It should have some other method for stopPublishing(topic) where it just sends an onCompleted on the stream. The service then has no awareness of mqtt as such and could be used just straight (with only one client) or it could be used with any other broker. There would be no pubsub without some kind of broker. The broker could be embedded in the server itself. If you want pubsub without a broker then just write a normal service that maintains a list of clients etc i.e. it does not use this Pubsub mechanism at all.
There could be a problem here with the subscription if it needs to be authenticated by the server. But there is no way to fix this. Once the server starts to publish to a well known topic then the only way to stop a client from subscribing to it is with mqtt policies because the client can just use raw paho to subscribe anyway. So the way this works is that is passed is only there for the call (initial setup of the subscription). Thereafter anyone who has the topic can subscribe.
If the MqttChannel that makes the call gets disconnected then the server will stop publishing. To avoid this the MqttChannel should be configured without an LWT.
TODO: All of this can be done on the client side by just changing the replyTo topic sent to the server. The MqttChannel should have subscribe method that takes a typed StreamObserver and keeps track of subscribers and drops the subscription when there are none left. Any other lifetime semantics should be implemented by the application. The purpose of this is just to give them the possibility of sharing the topic between multiple clients.

Or maybe we don't provide this. Just provide grpc services on the device that happen to go through mqtt. If something on the client side wants to broadcast this then let the client write that. They can use whatever they want to do it, it doesn't have to be the mqtt broker. In a lot of cases, if the stream has a lot of traffic, then they will forward it to kafka anyway. In practice in most systems there are not many cases for multicast. That's why udp is not used frequently. And if multicast is needed it can naturally be bolted on top of unicast. Layering it like this may make for a simpler system conceptually as the source of anything is always a single stream initially. 

Here is a description from https://fuchsia.dev/fuchsia-src/concepts/fidl/overview#messaging_models
Single request, multiple response
The multiple response case can be used in a "subscription" model. The client's message "primes" the server, for example, requesting notification whenever something happens.
The client then goes about its business.
Some time later, the server notices that the condition that the client is interested in has happened, and thus sends the client a message. From a client / server point of view, this message is a "reply", with the client receiving it asynchronously to its request.
There's no reason why the server couldn't send another message when another event of interest occurs; this is the "multiple response" version of the model. Note that the second (and subsequent) responses are sent without the client sending any additional messages.




## Topic structure

For the moment the topic structure is as follows

{server}/i|o/svc/{clientId}/{service}/{method}/{callId}

The client sends a request containing a requestId and a clientId (e.g. yyvkoydg5aepqxdn) to:

    device1/i/svc/helloservice/sayHello

Then server sends a reply to:

    device1/o/svc/yyvkoydg5aepqxdn/helloservice/sayHello/2gtknddwjbokgvfe
where,
    clientId = yyvkoydg5aepqxdn
    callId = 2gtknddwjbokgvfe


In general if the client has permissions to publish to server/i/service/method then it should be able to get permissions to subscribe to
server/o/{clientId}/#
However the MqttChannel can also be parameterised with a replyTopicPrefix for flexibility. 
In this case all replies for that MqttChannel will be sent to: 

    {replyToPrefix}/{service}/{method}/{callId}


--------------

In mqtt filtering is based on topic. This is inflexible because you cannot # subscribe something that you want to publish to.
'#' matches all following segments and can only be at the end of a filter. '+' matches one segment
So if you don't know what the future segment structure of things are and you think you may need to have a subscription that filters for all messages going to a particular device (maybe to log all activity for t device) then you have to have a structure like
server/i|o/svc/service/method
Then you can filter for server/i/# to get all device inbound traffic and this filter will not pick up the traffic sent out by the device itself.
If you do
server/svc/i|o/service/method
server/sys/i|o/service/method
Then you would need two filters
If you do
server/svc/service/i|o/method
Then to filter all you would need
server/svc/service/i/#
server/sys/service/i/#
or
server/+/service/i/#
If we could filter by attributes then we could have whatever topic structure we like that just filter for input=true or whatever
Microsoft use:
devices/{device-id}/messages/devicebound/

Also we are making point to point services so we want each MqttGrpcClient to be able to subscribe to something where it won't get messages that were supposed to be for other clients.


The structure will be:

    {server}/i|o/svc/all|{clientId}/service/method
    {server}/i|o/sys/all|{clientId}/service/method

Where 'all' just means all clients and if 'all' is not present then a specific client id has to be supplied. 
 
The client sends a request containing a requestId and a clientId (e.g. we2UL4O1SXyl5df7aY8bGA) to:

    device1/i/svc/all/helloservice/sayHello

Then server sends a reply to: 

    device1/o/svc/yyvkoydg5aepqxdn/helloservice/sayHello

With this scheme ('all' or clientId) it would also be possible to have a policy that restricts access to a service. i.e. You could grant read write access to the helloservice only with a topic policy like device1/+/svc/+/helloservice (although that would mean that the implementation of MqttGrpcClient would have to explicitly subscribe for each service separately instead of subscribing for device1/o/svc/#. Note that it could subscribe for each one separately on a just in time basis i.e. if a client tries to send a message to /helloservice then it would subscribe to device1/o/svc/clientId/helloservice on the fly. We won't do this initially, just subscribe to device1/o/svc/#)
What would be better is that the client is granted publish access to
    device1/i/svc/all/helloservice
and subscribe access to
    device1/o/svc/clientId/helloservice

Or more likely publish to:
device1/i/svc/all/*
and subscribe access to
device1/o/svc/clientId/*

Is there any problem in using the mqtt client id here? Probably not and it means that we can paramterise a single policy on {clientId}

The MqttChannel can be parameterised with any replyToPrefix. If not the default replyToPrefix will be {server}/o/svc/{clientId}
In this case replies will be at
{server}/o/svc/{clientId}/service/method
So forget about "all"
Requests are always at
{server}/i/svc/service/method

This makes it possible for the client to do a kind of pub sub if it wants to. So the client could specify replyToPrefix of myAnalytics
Then replies would go to 
myAnalytics/service/method.
and many things could subscribe to it 

If using the default then to be secure the client can have policy

pub server/i/svc/#
sub server/o/clientId/#


Note always that {server} does not have to be device1 it can be devices/device1


If a uuid is used for clientId there is no chance of one client guessing another's id.
The clientId could also be part of the call context (see security stuff) that any service could use 

If the clientId is used then there is no need for a uuid for callID because the call is unique on clientId and callId. So the callId could just be a short number (but we would have to hash on both callId and clientId)


## Status

The server status topic will be
    server/out/sys/status
The server will send a connected=true message to this when it starts up or if it when a client sends it a message on
    server/in/sys/status/prompt
The server will send a connected=false message to out/sys/status when it shuts down normally or when it shuts down abnormally via its LWT
The client status topic will be
    server/in/sys/status/client/{clientId}
so that clients can have restrictive policies. But the server will subscribe to
    server/in/sys/status/#
The client will send an empty message to in/sys/status/client/{clientId} when it shuts down normally or when it shuts down abnormally via its LWT. The server will then release any resources it has for {clientId} and send a cancel message to any call handlers.

The MqttChannel will have a waitForServer(int timeoutMillis) method. This will subscribe to server/out/sys/status and send a prompt to server/in/sys/status/prompt

## Security/Authentication

Note that we cannot secure the channel fully using JWT alone. This is because even if the device can reject an initial request the replies are published to a topic on the broker. So another client could wildcard subscribe to those. However because the topic contains the clientId it would not be hard to make policies for clients that only allow that client to access to that clients channels. Another way to do it would be to make a kubernetes service that exposes grpc over https. Then that service only has access to the broker. It forwards requests to the mqtt service.
https://stackoverflow.com/questions/58555788/generically-forwarding-a-grpc-call
https://github.com/ejona86/grpc-java/blob/grpc-proxy/examples/src/main/java/io/grpc/examples/grpcproxy/GrpcProxy.java

Note that the service could also do things like restarting watches etc. We needed some kind of service like this anyway.

gRPC supports sending a JWT

See the grpc-java github examples. To run it open
C:\dev\gitpublic\grpc-java\examples\example-jwt-auth
in intellij. Then make sure to check out a build tag (e.g. 1.47.0)
Then run ./gradlew installDist

In AuthClient.java they do:

`    HelloReply response =
        blockingStub
            .withCallCredentials(callCredentials)
            .sayHello(request);`

Note that the credentials will be populated in the MqttClientCall CallOptions

We could do something similar or the same on the stub and the generated stub code would pass the credentials to MqttGrpcClient

To put something in a context do
Context.Key<Integer> akey = Context.key("akey");
myContext = myContext.withValue(akey, 10);


In AuthServer they do

    `String clientId = Constant.CLIENT_ID_CONTEXT_KEY.get();`

This gets the context which is some kind of object stored in a threadLocal. We can easily do something similar this but it would be better to be able to reuse the gRPC Context class so any grpc service we write can also run in a classic grpc server. To understand this class put a breakpoint in Context.attach() and Run AuthServer and AuthClient. Up the call stack the system has a queue of runnables that it is running. Each one is a ContextRunnable. It's just a runnable that had some Context set in it as a .context member variable. Who knows where or when the runnable was created we don't care but we want to make sure that its context is put in thread local while it's running. So the run() of the runnable does Context.attach(). This puts the context into thread local. Then in a finally outside the run it makes sure to restore the previous context. Note that Context.attach() uses something called Storage but this is an instance of LazyStorage.storage whose createStorage() defaults to a Thread local. (they have some way of changing this but who knows if anyone even uses it). So we can use this. To create on there is a public static Context.ROOT. You create children from this using the with* methods. Then before doing a runnable make sure to set the context using .attach() (or use the Context.run(Runnable r) or Context.wrap() methods). Then in actual service implementation methods the service should be able to get e.g. an auth token from the Context.current() as in the example. Note that a context can also have a cancellation listene. so a method implementation could do Context.current() and register a listener on it and when the listener gets a cancel or deadline exceeded etc it could just stop and release resources or whatever. On the client side the deadline would be implemented some other way like waiting on a latch (or maybe using a context also? see the grpc impl of blocking stub with timeout or whatever). Anyway we would probably not support cancellation or timeouts initially.   

Note that in the grpc java implemenation they also allow you to add an interceptor that does authentication for all calls e.g. 

server = ServerBuilder.forPort(port)
.addService(new GreeterImpl())
.intercept(new JwtServerInterceptor())  // add the JwtServerInterceptor
.build()
.start();

But we would not have to do this initially. The user can just do it in their implementation of each service. Within their implementation they can just manually make a call out to their 'interceptor'. It would be better if we do allow it though as it means that they can guarantee that every service will be authenticated by default.
Also if we look at the ServerBuilder interface nearly all of it's methods (aside from things like intercept()) make no sense for the mqtt server. So we could leave out the builder pattern altogether or maybe just have a much simpler builder. We would just need intercept, setMqttConnection, setLwt or whatever. Maybe we should just have these as constructor parameters first and then later refactor them to a builder. 

See this article for examples of interceptors on client and server side (also C:\gitpublic\interceptors\grpc\grpc-unary-rpc)

In that example in GrpcServer.java they do:

    this.server =
        ServerBuilder.forPort(port)
            .addService(
                ServerInterceptors.intercept(
                    productService,
                    new GrpcServerResponseInterceptor(),
                    new GrpcServerRequestInterceptor()))
            .build();

This actually works with the MqttServer. 

## Error handling
Use this https://cloud.google.com/apis/design/errors
Note that it has it's own protobuf Status https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
This can be used for transport
See also this section https://cloud.google.com/apis/design/errors#error_payloads
They have different kinds of error payloads for different kinds of errors
Also there was some note about limiting the number of error types as clients will have to write a lot of code to handle them (the error handling code ends up bigger than the logic) so maybe we should stick to the google Code types
See StatusProto.java#fromThrowable. This assumes that somewhere in the stack is a StatusRuntimeException or a StatusException. So any service we write that wants to send errors must always construct one of these types of exceptions and call onError. ********Need to document this somewhere for clients*********



Mqtt connection error handling:

***** See notes under Status heading above*****
The mgClient should send an initial connection request to the service. If it gets a response then the service will send back its lwt topic and the mgClient can listen for it and then send errors to streams. If it gets no response within a timeout then it assumes that the service is not registered or the server is not running (although if we didn't use timeouts we could distinguish between these cases - but there are other ways to distinguish and anyway in most cases we will know that it is the server that is down because we will know that that server always has those services running - anyway we can provide another way later to get a list of services from a server and their status etc.)  The mgClient remains subscribed to this topic and listens for 'on'/'off' in the status message. In the intitial connection request the mgClient can send it's lwt to the server and the server can send errors to client streams. If this becomes very complicated then we could just time out client streams instead. i.e. if we don't receive a message in a client stream for some time then timeout the stream and fail. This could be much simpler and may match with practice i.e. In practice a client stream should be rapid. It's not for sending rare requests. For rare requests the client should just send single requests. If the server wants to 'watch' something on the client then it should call a service on the client. Along with this for our situtation the client should rarely lose contact with the broker as the client and broker run on the cloud. The device is very different in this regard.
There is a difference between the client noticing that it itself has been disconnected from the mqtt broker and telling the server that it has been disconnected. We should maybe handle the start case? We will have to handle the start case on the server anyway so we could use similar code.
The mqtt connection will need to notify a number of entities whenever it is disconnected/re-connected.

//We could do options.setCleanSession(false) here which will make a persistent session
//For AWS the "Persistent session expiry period" is extendable up to 7 days.
//https://docs.aws.amazon.com/general/latest/gr/iot-core.html#message-broker-limits
//If that is reliable then we won't need any re-start notifications
//as all subscriptions will be re-constituted. It will even deliver queued messages (but for grpc services
//we probably would not want that. A stream should complete or not, not be in a half way state).
//In that sense the clean session is easier. For example we don't want some sub to a random replyto to be
//re-created. In fact the only sub we want re-created would be for something like a watch.
//So watches might be done on a separate persistent session (clean=false)
//But even with watches the main connection that will fail is the device/server and that is publishing
//So it has to remember to publish anyway and persistent session will not help with that.
//i.e. The only place the persistent session would be valuable is the cloud watch client which is unlikely to fail
//So overall it's better to keep it 'dumb pipes' except for setting the re-connect automatically and notifiying
//each grpc client and server when a connect or disconnect happens.


Why not model the server up front? There is a server at a topic and that has services at subtopics. This works for the internet with domains and sub domains etc. With devices we have to have a topic anyway. Maybe later we might want to hide devices behind some other indirect topic? We could still make the server an indirect topic if we wanted to. So there is a server at a topic. Then the client connects to the server and listens for its lwt at a well known topic. It can also query the server for services it supports etc. The alternative to this is just sending a connectionStatusRequest to service/connectionStatusRequest and then timing out if there is no response.
If we do this then it breaks the mqtt anonymity of who responds to a message etc. But we are modeling services here like grpc and grpc is able to solve a lot of problems. For pubsub etc then just use mqtt straight.
Maybe the ability to pass service topics around is something that is worth it. So you don't know about the server. You just get a service topic and work away.
Theres an mgClient which has the root topic ("device1") for a server and is mapped 1:1 to that server. Then it can listen for the server's status. Then the user can make a stub with the mgClient and a subtopic for the service ("datasource1/helloservice"). The mgClient will put these together as device1/svc/datasource1/helloservice. i.e. it appends the "svc" to it. Then use a similar pattern on the server.
The mgClient (ProtoSender) will maintain a map of listeners. It removes a listener wherever it currently does an unsubscribe. If it gets an lwt from the server it is mapped to then it sends an error to each listener, removes and unsubscribes them.
Note that one good thing about the service approach is that each stub gets individually informed when there is a disconnect and can take action/clean up etc. With normal mqtt there was no real channel for doing that.
Later it may also sent its lwt topic to the server when it initially connects. It will send this along with a client uuid. Then in every message it sends it also sends the client id. The server can then maintain a map of client stream listeners vs client ids and if it gets an lwt it can send on an error to all the client stream listeners. It could also use this same idea to handle cancellations (see the grpc timeouts stuff below. If a client times out then it should send some kind of cancel to the clientStream which is send as onError with a CANCELLED status). There does not seem to be anything about telling servers to stop sending server streams if the client is gone. So the server stream will probably just continue until finished.
On the client side have to take care of both the server mqtt connection by listening to lwt but also the client mqtt connection.
If the client connection fails then any streams also have to be sent an error. It would be possible to have the MqttGrpcClient own the mqtt connection. But we don't want to do that as we want to be able to use one mqtt client for many devices. So we will have to have some way of notifying each MqttGrpcClient when an mqtt client is disconnected.
Should the client try to automatically re-connect? If it does then the MqttGrpcClient will also have to re-connect for the lwt message.

If a request fails on the client side should the client just throw an exception? i.e. instead of calling onError on the response observer. No. In general propagate the exceptions through onError().
TODO: Just do the above as a prototype. It won't take that long anyway. i.e. implement the topic structure and what is described in the paragraph above.

For requests timeouts could work? We only need the lwt for streams. Even then timeouts could work (except for things like watch but watch will be separate anyway). But the problem with timeouts is that if the client can set them then they may set a long one (especially for automation where it might take a peripheral a while to respond to some complex request) and it has no idea whether the peripheral failed to respond in the timeout or whether the mqtt connection is down on the server side or whether the service even exists. "How can the client cancel a stream from the server part way through when using the async stub? For single requests, you can't. For bi-directional streaming you can call onError if you have not already called onCompleted. The API is simply too simple for this need; this problem would be solved by using more reactive-streams than RxJava, but that has its own issues."

TODO: timeouts. grpc supports these. See https://www.tutorialspoint.com/grpc/grpc_timeouts_and_cancellation.htm
and https://grpc.io/docs/what-is-grpc/core-concepts/#deadlines
Note that some languages work in terms of deadlines and some in terms of timeouts
We can support them easily enough in StreamWaiter and StreamIterator. The BlockingStub might be harder though.

TODO: subscription limits (because of build up of replyTo topics with long running requests)
AWS Limits: 50,000 concurrent connections per account. 50 subscriptions per connection (also 50,000 subs per account)
So we can scale to many devices if we have a few devices per connection
But we cannot scale non timing out request response where each response has a unique replyTo
Neither can we scale ongoing streaming responses where each has a unique replyTo
So we at least need a request id and switch on that.
After we probably need a subscription per service. If there are permissions on particular methods then we can't use aws topic permissions for that (which is unlikely anyway, the most we will probably have is a restriction on the tenant).
In fact we should probably just have a single subscription for all services on a device and then filter by topic. We can still have a unique replyTo for each request because we will just be using a single wildcard subscription. Or we can use a requestId. Might as well use a requestId as we already have a streamId which can be used. The only reason not to would be maybe to see cloudwatch logs of requests without decoding the protobuf.

TODO: write tests using mockito fake or whatever (the one that extends a real instance) and check for leaks in MqttGrpcClient.responseObservers etc.

grpc core documented here:
https://grpc.io/docs/what-is-grpc/core-concepts/
This describes what is happening on the wire in detail (Chapter 4. gRPC under the hood. (Also in c:\books))
https://www.oreilly.com/library/view/grpc-up-and/9781492058328/ch04.html
Also the diagrams in this are useful for showing what the streams look like on http/2 (ignore the code it's just the implementation of the route example in ballerina)
https://thenewstack.io/grpc-a-deep-dive-into-the-communication-pattern/
Error handling:
https://www.baeldung.com/grpcs-error-handling
https://techdozo.dev/getting-error-handling-right-in-grpc/
https://www.grpc.io/docs/guides/error/
Note that the google stubs favour StatusRuntimeException - probably just copy this for the moment but we could change that.


Effectively each method has an input and output stream because of how grpc is implemented over http/2 so there are only 4 types of method. The only place where this two stream restriction causes problems is for the stream in stuff. It would be nice to be able to pass a parameter here also that says things about what you are streaming in. But input streams are unusual enough and you can handle it by putting an optional object in the start message that has the parameters for the rest of the stream:
single in, single out
single in, stream out
stream in, single out
stream in, stream out

single in, single out
rpc GetFeature(Point) returns (Feature) {}
public void getFeature(Point request, StreamObserver<Feature> responseObserver)

single in, stream out
rpc ListFeatures(Rectangle) returns (stream Feature) {}
public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver)

stream in, single out
rpc RecordRoute(stream Point) returns (RouteSummary) {}
public StreamObserver<Point> recordRoute(final StreamObserver<RouteSummary> responseObserver)

stream in, stream out
rpc RouteChat(stream RouteNote) returns (stream RouteNote) {}
public StreamObserver<RouteNote> routeChat(final StreamObserver<RouteNote> responseObserver)

The single out methods may look awkward but the benefit is that it is clear that this is not a local inline request.
You will have to at least wrap it in a StreamSingleWaiter and put in a timeout.

stream in, single out may not be great for e.g. download project where you might like to pass some extra info besides the stream, e.g. the name of the project etc. So for this you might have to have some optional metadata in the start message. Normally it will take all the values in the stream and then return the single out but if there is a problem then it will return the single out straight away with an error (which client gets as exception).
Also are the grpc streams stateless? i.e. what if you have multiple clients streaming in. Does the server have to distinguish each stream? Presumably not. So we should test for the same.

For stream in the start request must supply a replyTo, a streamId and no message
Then the stream requests must supply a streamId a message and an empty replyTo

Pub Sub
Assume that Pub sub will only work over mqtt. i.e. If you want publish subscribe then you need to make sure you include a message broker. In that case the client can just just the mqtt api more or less directly to do things. So for the desktop watch it will be modeled as one method which does create watch with an id another output stream method that listens for that watch. That uses the grpc style of stuff. But we can make another api that assumes there is a broker underneath. That api can be generic. For anything that does streaming it can take an extra parameter which is the stream topic. So when you create a watch you can supply the stream topic as one of the create properties. Then we will have a separate api that you can call where you just subscribe for a topic. Your code will then expect to get watches on that topic or whatever. Maybe we should do ref counting as well. Whatever. The main point is that for this kind of thing we won't worry about sticking exactly to grpc because grpc doesn't support pubsub as a pattern anyway.
In detail: Have a method WatchService.createWatch(WatchCreate). This takes a WatchCreate message/protobuf that has the watch variables, update rate and a values topic. It returns success or failure. Then have a different method that is not defined in IDL SubscribeService.subscribe(String topic, Parser<T> parser, StreamObserver<T> streamObserver). That just listens for values. All the createWatch, deleteWatch, registerWatch etc. is done on the WatchService. But have to take care of too many subscriptions and many subscriptions on same topic from one client. Maybe just use the watchID or requestID and multiplex like the BlockingRequestShared stuff?


It might be realistic for us to say that if it is not in process then we will always use mqtt. In that case we could possibly simplify things and we could always be sure that onReply would work (without having to plug something in to our service that calls onNext followed by onCompleted)

If we make our own style of interface instead of strict grpc then it should not matter as long as that interface is easily adaptable to something that only has a single input stream and single output stream of protocol buffers. This can then be wrapped in a grpc service. So even if our interface uses e.g. onSingle() then the wrapper can translate that as onNext followed by onCompleted. The only place the two streams solution is a little awkward is for a client input stream because then the input stream has taken the slot of something where you can pass parameters. But this is an unusual case and can be dealt with by having an optional protobuf in the start message. (Of course in our mqtt imlementation the client input stream is easilty separated from the start parameter as the input stream needs a unique stream id whereas the start parameter just has a replyTo - maybe replyTo should be requestId == streamId - no it should be separate so we can do pubsub and batching. But we should do it like pilz iot where the listener will separate stuff based on requestId and not just rely on the topic).

----------------------------

Making RouteGuide work as if it needed to be used in a desktop and remotely

Will extract an IRouteGuide interface
Then make a RouteGuideStub that implements this (they use stub where com uses proxy, so we will use stub for client and skeleton for server like corba)
The RouteGuideStub does the remote comms
Then make a RouteGuideBlockingStub that wraps an IRouteGuideStub and makes simpler blocking calls to the service whether it is local or remote.

Note that the stub is expected to throw a statusruntimeexeption which is unchecked

---------------

protoc compliler plugin:
It should be not too hard to get the grpc compiler to generate the stub and skeleton from the grpc definition of the service.
Probably should get a student to do this.
It could be done as a grpc compiler plugin. See the https://github.com/vert-x3/vertx-grpc-java-compiler
Can write a plugin in any language: https://scalapb.github.io/docs/writing-plugins/
Google: creating a protoc plugin in java
Simple example in java: https://github.com/thesamet/protoc-plugin-in-java
The above uses a bat file to get protoc to call it but below has something about how get it to be executable:
https://stackoverflow.com/questions/56414734/create-custom-protoc-plugin-in-java
Also can look at the source of com.google.protobuf:protobuf-java
But could also just modify the protoc compiler see
https://github.com/protocolbuffers/protobuf/tree/main/src/google/protobuf/compiler/java