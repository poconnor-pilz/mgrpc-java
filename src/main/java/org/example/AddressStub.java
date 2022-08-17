package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

public class AddressStub implements IAddressService{

    final ProtoSender protoSender;

    public AddressStub(ProtoSender protoSender) {
        this.protoSender = protoSender;
    }

    @Override
    public void handlePerson(Person person, StreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {

        protoSender.sendRequest(IAddressService.METHOD_PERSON, person,
                new BufferToStreamObserver<>(SomeRequestOrReplyValue.parser(), responseStream));

    }

    @Override
    public void handleAddress(AddressBook book, StreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {
        protoSender.sendRequest(IAddressService.METHOD_ADDRESS, book,
                new BufferToStreamObserver<>(SomeRequestOrReplyValue.parser(), responseStream));
    }

    @Override
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, StreamObserver<Person> responseStream) throws Exception {
        protoSender.sendRequest(IAddressService.METHOD_SERVER_STREAM_PERSONS, requestVal,
                new BufferToStreamObserver<>(Person.parser(), responseStream));
    }




    @Override
    public StreamObserver<Person> clientStreamPersons(StreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {

        final String streamId = Base64Utils.randomId();

        protoSender.sendClientStreamingRequest(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId,
                new BufferToStreamObserver<>(SomeRequestOrReplyValue.parser(), responseStream));

        return new StreamObserverToSender<>(protoSender, IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId);

    }
}
