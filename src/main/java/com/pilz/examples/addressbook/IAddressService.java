package com.pilz.examples.addressbook;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import io.grpc.stub.StreamObserver;

public interface IAddressService {

    String PERSON = "person";
    String ADDRESS = "address";
    String SERVER_STREAM_PERSONS = "serverStreamPersons";
    String CLIENT_STREAM_PERSONS = "clientStreamPersons";

    //Single in, single out
    void handlePerson(Person person, StreamObserver<SomeRequestOrReplyValue> replyStream);

    void handleAddress(AddressBook book, StreamObserver<SomeRequestOrReplyValue> replyStream);

    //Single in, stream out
    void serverStreamPersons(SomeRequestOrReplyValue requestVal, StreamObserver<Person> personStream);

    //Stream in, single out
    StreamObserver<Person> clientStreamPersons(StreamObserver<SomeRequestOrReplyValue> responseStream);
}
