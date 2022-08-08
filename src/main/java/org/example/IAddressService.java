package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;

public interface IAddressService {

    String METHOD_PERSON = "person";
    String METHOD_ADDRESS = "address";

    String METHOD_SERVER_STREAM_PERSONS = "serverStreamPersons";

    String METHOD_CLIENT_STREAM_PERSONS = "clientStreamPersons";

    //Single in, single out
    SomeRequestOrReplyValue handlePerson(Person person) throws Exception;

    SomeRequestOrReplyValue handleAddress(AddressBook book) throws Exception;

    //Single in, stream out
    void serverStreamPersons(SomeRequestOrReplyValue requestVal, MqttProtoStreamObserver<Person> personStream)
            throws Exception;

    //Stream in, single out
    MqttProtoStreamObserver<Person> clientStreamPersons(MqttProtoStreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception;
}
