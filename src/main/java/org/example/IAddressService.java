package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import io.grpc.stub.StreamObserver;

public interface IAddressService {

    String METHOD_PERSON = "person";
    String METHOD_ADDRESS = "address";

    String METHOD_SERVER_STREAM_PERSONS = "serverStreamPersons";

    String METHOD_CLIENT_STREAM_PERSONS = "clientStreamPersons";

    //Single in, single out
    void handlePerson(Person person, StreamObserver<SomeRequestOrReplyValue> replyStream) throws Exception;

    void handleAddress(AddressBook book, StreamObserver<SomeRequestOrReplyValue> replyStream) throws Exception;

    //Single in, stream out
    void serverStreamPersons(SomeRequestOrReplyValue requestVal, StreamObserver<Person> personStream)
            throws Exception;

    //Stream in, single out
    StreamObserver<Person> clientStreamPersons(StreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception;
}
