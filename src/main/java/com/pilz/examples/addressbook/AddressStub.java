package com.pilz.examples.addressbook;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.pilz.mqttgrpc.*;
import io.grpc.stub.StreamObserver;

public class AddressStub implements IAddressService {

    final ProtoSender sender;

    public AddressStub(ProtoSender sender) {
        this.sender = sender;
    }

    @Override
    public void handlePerson(Person person, StreamObserver<SomeRequestOrReplyValue> responseStream) {

        sender.sendRequest(IAddressService.PERSON, person,
                new StreamToBufferObserver<>(SomeRequestOrReplyValue.parser(), responseStream));

    }

    @Override
    public void handleAddress(AddressBook book, StreamObserver<SomeRequestOrReplyValue> responseStream){
        sender.sendRequest(IAddressService.ADDRESS, book,
                new StreamToBufferObserver<>(SomeRequestOrReplyValue.parser(), responseStream));
    }

    @Override
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, StreamObserver<Person> responseStream){
        sender.sendRequest(IAddressService.SERVER_STREAM_PERSONS, requestVal,
                new StreamToBufferObserver<>(Person.parser(), responseStream));
    }




    @Override
    public StreamObserver<Person> clientStreamPersons(StreamObserver<SomeRequestOrReplyValue> responseStream) {

        return sender.sendClientStreamingRequest(IAddressService.CLIENT_STREAM_PERSONS,
                new StreamToBufferObserver<>(SomeRequestOrReplyValue.parser(), responseStream));

    }
}
