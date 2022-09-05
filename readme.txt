todo: For pub/sub let the client specify the replyTo. pub/sub is assumed to never send an end of stream.
Also the proxy doesn't get a return value. It must subscribe separately through another api.
i.e. It's a different api, different proxy type of code that the user has to do manually (with maybe the help of replyTo etc and some helper classes to put that together)


Try prototype where signature of method is either

request response:

AVal aMethod(AParam param)

Server stream (or async request response)

void aMethod(AParam param, StreamObserver<AVal> valueStream)

Client stream

StreamObserver<AVal> aMethod(AParam param)

Should the StreamObserver s be Queues? Maybe not because we need a StreamObserver.onLast() and a queue doesn't have that unless we can put a termination message in it.

Advice here for multiple messages to put size of message before the message (ala geo binary )
https://developers.google.com/protocol-buffers/docs/techniques

proto stub signature:

IProtoListener onProtoRequest(String topic, byte[] protoRequest, IProtoListener replies)

IProtoListener{
	void onNext(byte[] protobuf);
	void onLast(byte[] protobuf);
}

Note that for grpc they have to send an end of stream message even for a single response
This is to close the stream.
But we don't close streams in mqtt
We do drop subscriptions so we need to have an onLast() but we don't want an onNext() followed by onEnd()
because that would mean sending two messages.
Maybe we will need something later for batching but we definitely don't want to batch request response.

We should try for smart endpoints, dumb pipes. So let the final user client code deal with exceptions and lost connections and re-connecting etc.

The client side stuff (proxy class etc) should be functional and stateless. A proxy is just a functor that sends stuff to a topic that you parameterise
The server side stuff is necessarily stateful as it maintains a connection to a particular opc server. It starts up and listens.

Responsibility of proxy and stub classes is just to map method names and protobufs to actual methods and typed messages.

TODO: Copy grpc and have all exceptions as exceptions. Propagated exceptions that happen remotely via the error (possibly call it exception)
If a service wants to return a user level error then it should encode that in the reply?

TODO: Do we want to later allow sending multiple reponses (even for different services) to same replyTo? This would cut down on topics
In this case we will need to model it on BlockingRequestSharedReplyTo which uses a request id to distinguish.

TODO: Consider making the basic service asynchronous in terms of IStreamListenser<V>. Then write a local proxy that wraps that for synch calls. Then write a remote proxy that does the same (or does both, an asynch and a blocking synch or whatever). This code could be generated later if we want to. It could be generated from the base service interface or it could be generated from a grpc service definition if we modify the compiler. Because the grpc interface definition has exactly what we need to distinguish between streams and values e.g. rpc RecordRoute(stream Point) returns (RouteSummary) {}. ***** But making the base service asynch won't really work as we don't want everything to return a stream (for potential client side streaming).

TODO: Possibly consider supporting onNext() and onCompleted() for streamed reply and then just onReply() for normal single replies to be more like grpc for streamed responses? -Maybe use to this because the programming of onLast is awkward for server and client for non unary- Maybe no to this because we will probably have to support out of order anyway. Mqtt is too different. You could force it to be like grpc but we don't need that so it's the wrong tool for the job. As long as we have protobufs that should be enough of a lowest common denominator anyway. If we did have to build a grpc server on top of this then the onLast handler for the mqtt could just call onNext followed by OnCompleted for the upper level grpc client.
The problem with not supporting onCompleted() is that you can never take any existing grpc service and migrate it without changning it's implementation code (replacing onNext and onCompleted with onLast, and onNext could be part of a loop where you now have to not do the last thing in  the loop - very awkward). We will support ordering of all messages (whether next or completed) so that can guarantee that completed is handled after next. So we should change to do onNext and onCompleted and then add onReply as an optimisation if we think it is necessary. Then later if we want to migrate our services to grpc over http we could take the onReply handler and get it to call onNext followed by onCompleted.
We may want grpc compatibility for our own use cases. For example if pas on a desktop wants to communicate with a remote device. It could do this over http with grpc. It means that it would not need to make an mqtt connection.
TODO: Use enum to represent message type

TODO: What about out of order messages in a stream? We should probably support a counter based id for this. It can be per stream as the stream will be distinguished by topid or if there is a shared topic then by something like a watchId or requestId. For request response ('unary' in grpc terms) can just set it to zero. Ordering will have to be managed as for services it will be important. For example a service that streams a project down to a device. So ordering should be built in. Look at the google mqtt javascript client code for this.
For google iot the suggestion for ordering is here:
https://web.archive.org/web/20190629192452/https://cloud.google.com/pubsub/docs/ordering
But this just buffers messages until a timeout and then sorts those and processes them. They could have done better than this by only buffering as soon as a messsage that doesn't have a counter of previous+1 arrives. But maybe this is because they can't guarantee that all messages are delivered (which mqtt does).
Note that the latest version of this doc states that you can specify to the pubsub system that your message has an ordering key and it will order the messages delivered to you. But this version of the doc basically says to get all the messages in a stream (or up to a timeout here) and then order them all before dealing with them.
Is there a way of just doing this sorting only if the next message does not have the last message id+1? Then start buffering messages until you get the ones you are missing. But what if in the meantime there are more messages coming in out of order. When do you stop? Just make the buffer and keep sorting it until all the messages are one plus the last. Then send on that buffer's worth. Do this in a blocking function so you don't receive new messages while sorting. Maybe in the sending on bit have a blocking queue that you send the ordered messabes to and a separate thread to service that in case the client is slow. Also overall for the sorting have a timeout so that if you don't get ordered messages within a certain time you just fail. Also throw away duplicates. Overall this should not happen frequently and the out of order message should appear very soon as it's just a network delay. If qos is 1 then you are guaranteed to get it at least once. There should be no waiting for ages for it. The fact that you got message 3 before 2 means that 2 was sent already so it's not like you are waiting for the server to send it. See TryOrdering.java for this.

TODO: Consider requestId and shared replyTo like pasiot. This can be important for batching where there are many different watches. But the client will have to know to unsub from the topic.

TODO: Refactor watch protocol to use grpc like semantics as google does with their grpc interface to pubsub. The createWatch is just a request response. From there you use the watchId to subscribe to a stream. And you can have many clients subscribe to the same stream. But for this to be efficient it should use a shared topic with reference counting etc

TODO: How do we implement stream cancellation?

TODO: pubsub. First this will only work with mqtt. It won't work locally. So the Service implementation will need an mqtt connection. Then we will make a general subscription client with the method subscribe(String topic, StreamObserver<V> responseObserver). Note that this will have to work around the fact that paho will only have one subscriber in a particular jvm (i.e. it doesn't do pubsub withing jvm) Then we make the actual service like a WatchService. This just has request response methods for making watches and in the request is the response topic for the watch values. These can be batched etc. Note that when something unsubscribes it should probably get an onCompleted() in order to maintain the semantics of grpc and allow it to clean up or whatever.

Error handling:
Will use this https://cloud.google.com/apis/design/errors
Note that it has it's own protobuf Status https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto
This can be used for transport
See also this section https://cloud.google.com/apis/design/errors#error_payloads
They have different kinds of error payloads for different kinds of errors
Also there was some note about limiting the number of error types as clients will have to write a lot of code to handle them (the error handling code ends up bigger than the logic) so maybe we should stick to the google Code types
See StatusProto.java#fromThrowable. This assumes that somewhere in the stack is a StatusRuntimeException or a StatusException. So any service we write must always construct a one of these and call onError. ********Need to document this somewhere*********


Topic structure
In mqtt filtering is based on topic. This is inflexible because you cannot # something that you want to publish to.
# matches all following segments and can only be at the end of a filter. + matches one segment
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

For the moment just make an api that has the server address and then the service name and service method and let the api put them togehter in a topic
We can then restructure the topic later.

The initial structure will be
server/i|o/svc/service/method
server/i|o/sys/service/method
server/o/sys/status/getStatus
where getStatus is a stream that the client can listen to and is wired to the lwt of the device. It just sends it an empty request.
Go with this initially as it matches the azure setup and that works for them.
Also it is some way consistent. Every device has an input and output channel. When you plug in a service you just supply your service/method and it is just appended to the input or output channel and sent to/from your service.
For permissions though the user will either have to be granted two or else one that can match on server/+/svc/service/method

We will support more than one hello service on a device or wherever. Note that grpc seems to only support one instance of a particular
service type on a server (i.e. at a particular domain name - but at least they can do domain names and subdomains whereas we just have one global
broker so we want to have the facility to put things at a particular topic).
See the spec here https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
Note also that "server" above could contain a few subtopics.
We may also have
server/sys/services
which is a service that can return things about the server e.g. the list of services running and their topics and types etc.
And finally
server/sys/status
Which is the status topic and the lwt topic.
Should we consider the azure structure of: devices/{device-id}/messages/devicebound/, or devices/{device-id}/messages/devicebound/{property-bag} when there are message properties. {property-bag} contains url-encoded key/value pairs of message properties
The main thing is that we need to distinguish input and ouput so that a server can subsribe for a wildcard but not end up receiving its own sent messages.
If we put the i|o after the service then it means that each service has to have its own subscription and we can't do any generic filtering on a services messages (well we can do generic read but not generic read and write).
Also remember that if this needs to be changed because the topic structure is too inflexible that we could put more in the mqttgrpc message attributes.

Mqtt connection error handling:
The protoSender should send an initial connection request to the service. If it gets a response then the service will send back its lwt topic and the protoSender can listen for it and then send errors to streams. If it gets no response within a timeout then it assumes that the service is not registered or the server is not running (although if we didn't use timeouts we could distinguish between these cases - but there are other ways to distinguish and anyway in most cases we will know that it is the server that is down because we will know that that server always has those services running - anyway we can provide another way later to get a list of services from a server and their status etc.)  The protoSender remains subscribed to this topic and listens for 'on'/'off' in the status message. In the intitial connection request the protoSender can send it's lwt to the server and the server can send errors to client streams. If this becomes very complicated then we could just time out client streams instead. i.e. if we don't receive a message in a client stream for some time then timeout the stream and fail. This could be much simpler and may match with practice i.e. In practice a client stream should be rapid. It's not for sending rare requests. For rare requests the client should just send single requests. If the server wants to 'watch' something on the client then it should call a service on the client.

Why not model the server up front? There is a server at a topic and that has services at subtopics. This works for the internet with domains and sub domains etc. With devices we have to have a topic anyway. Maybe later we might want to hide devices behind some other indirect topic? We could still make the server an indirect topic if we wanted to. So there is a server at a topic. Then the client connects to the server and listens for its lwt at a well known topic. It can also query the server for services it supports etc. The alternative to this is just sending a connectionStatusRequest to service/connectionStatusRequest and then timing out if there is no response.
If we do this then it breaks the mqtt anonymity of who responds to a message etc. But we are modeling services here like grpc and grpc is able to solve a lot of problems. For pubsub etc then just use mqtt straight.
Maybe the ability to pass service topics around is something that is worth it. So you don't know about the server. You just get a service topic and work away.
Theres an mgClient which has the root topic ("device1") for a server and is mapped 1:1 to that server. Then it can listen for the server's status. Then the user can make a stub with the mgClient and a subtopic for the service ("datasource1/helloservice"). The mgClient will put these together as device1/svc/datasource1/helloservice. i.e. it appends the "svc" to it. Then use a similar pattern on the server.
The mgClient (ProtoSender) will maintain a map of listeners. It removes a listener wherever it currently does an unsubscribe. If it gets an lwt from the server it is mapped to then it sends an error to each listener, removes and unsubscribes them.
Note that one good thing about the service approach is that each stub gets individually informed when there is a disconnect and can take action/clean up etc. With normal mqtt there was no real channel for doing that.
Later it may also sent its lwt topic to the server when it initially connects. It will send this along with a client uuid. Then in every message it sends it also sends the client id. The server can then maintain a map of client stream listeners vs client ids and if it gets an lwt it can send on an error to all the client stream listeners. It could also use this same idea to handle cancellations (see the grpc timeouts stuff below. If a client times out then it should send some kind of cancel to the clientStream which is send as onError with a CANCELLED status). There does not seem to be anything about telling servers to stop sending server streams if the client is gone. So the server stream will probably just continue until finished.

TODO: Just do the above as a prototype. It won't take that long anyway. i.e. implement the topic structure and what is described in the paragraph above.

For requests timeouts could work? We only need the lwt for streams. Even then timeouts could work (except for things like watch but watch will be separate anyway). But the problem with timeouts is that if the client can set them then they may set a long one (especially for automation where it might take a peripheral a while to respond to some complex request) and it has no idea whether the peripheral failed to respond in the timeout or whether the mqtt connection is down on the server side or whether the service even exists. "How can the client cancel a stream from the server part way through when using the async stub? For single requests, you can't. For bi-directional streaming you can call onError if you have not already called onCompleted. The API is simply too simple for this need; this problem would be solved by using more reactive-streams than RxJava, but that has its own issues."

TODO: timeouts. grpc supports these. See https://www.tutorialspoint.com/grpc/grpc_timeouts_and_cancellation.htm
and https://grpc.io/docs/what-is-grpc/core-concepts/#deadlines
Note that some languages work in terms of deadlines and some in terms of timeouts
We can support them easily enough in StreamWaiter and StreamIterator. The BlockingStub might be harder though.

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


Effectively each method has an input and output stream because of how grpc is implemented over http/2 so there are only 4 types of method. The only place where this two stream restriction causes problems is for the stream in stuff. It would be nice to be able to pass a parameter here also that says things about what you are streaming in. But input streams are unusual enough and you can handle it by putting an optional object in the first message that has the parameters for the rest of the stream:
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

stream in, single out may not be great for e.g. download project where you might like to pass some extra info besides the stream, e.g. the name of the project etc. So for this you might have to have some optional metadata in the first message. Normally it will take all the values in the stream and then return the single out but if there is a problem then it will return the single out straight away with an error (which client gets as exception).
Also are the grpc streams stateless? i.e. what if you have multiple clients streaming in. Does the server have to distinguish each stream? Presumably not. So we should test for the same.

For stream in the first request must supply a replyTo, a streamId and no message
Then the stream requests must supply a streamId a message and an empty replyTo

Pub Sub
Assume that Pub sub will only work over mqtt. i.e. If you want publish subscribe then you need to make sure you include a message broker. In that case the client can just just the mqtt api more or less directly to do things. So for the desktop watch it will be modeled as one method which does create watch with an id another output stream method that listens for that watch. That uses the grpc style of stuff. But we can make another api that assumes there is a broker underneath. That api can be generic. For anything that does streaming it can take an extra parameter which is the stream topic. So when you create a watch you can supply the stream topic as one of the create properties. Then we will have a separate api that you can call where you just subscribe for a topic. Your code will then expect to get watches on that topic or whatever. Maybe we should do ref counting as well. Whatever. The main point is that for this kind of thing we won't worry about sticking exactly to grpc because grpc doesn't support pubsub as a pattern anyway.


It might be realistic for us to say that if it is not in process then we will always use mqtt. In that case we could possibly simplify things and we could always be sure that onReply would work (without having to plug something in to our service that calls onNext followed by onCompleted)

If we make our own style of interface instead of strict grpc then it should not matter as long as that interface is easily adaptable to something that only has a single input stream and single output stream of protocol buffers. This can then be wrapped in a grpc service. So even if our interface uses e.g. onSingle() then the wrapper can translate that as onNext followed by onCompleted. The only place the two streams solution is a little awkward is for a client input stream because then the input stream has taken the slot of something where you can pass parameters. But this is an unusual case and can be dealt with by having an optional protobuf in the first message. (Of course in our mqtt imlementation the client input stream is easilty separated from the first parameter as the input stream needs a unique stream id whereas the first parameter just has a replyTo - maybe replyTo should be requestId == streamId - no it should be separate so we can do pubsub and batching. But we should do it like pilz iot where the listener will separate stuff based on requestId and not just rely on the topic).

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