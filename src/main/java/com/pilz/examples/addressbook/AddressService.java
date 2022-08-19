package com.pilz.examples.addressbook;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.pilz.mqttgrpc.Logit;
import io.grpc.stub.StreamObserver;

public class AddressService implements IAddressService {


    @Override
    public void handlePerson(Person person, StreamObserver<SomeRequestOrReplyValue> replyStream){
        log("handlePerson");
        log(person.toString());
        final SomeRequestOrReplyValue reply = SomeRequestOrReplyValue.newBuilder().setTheVal("handled " + person.getName()).build();
        replyStream.onNext(reply);
        replyStream.onCompleted();
    }

    @Override
    public void handleAddress(AddressBook book, StreamObserver<SomeRequestOrReplyValue> replyStream){
        log("handleAddress");
        log(book.toString());
        final SomeRequestOrReplyValue reply = SomeRequestOrReplyValue.newBuilder().setTheVal("got an address ").build();
        replyStream.onNext(reply);
        replyStream.onCompleted();

    }

    @Override
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, StreamObserver<Person> personStream){
        log("serverStreamPersons with request value of " + requestVal.getTheVal());
        int numPersons = 3;
        for(int i=0; i < numPersons; i++){
            Person person =
                    Person.newBuilder()
                            .setId(i)
                            .setName("Person" + i)
                            .setEmail(i+ "@example.com")
                            .addPhones(
                                    Person.PhoneNumber.newBuilder()
                                            .setNumber("555-" + i)
                                            .setType(Person.PhoneType.HOME))
                            .build();

                personStream.onNext(person);
        }
        personStream.onCompleted();

    }

    @Override
    public StreamObserver<Person> clientStreamPersons(StreamObserver<SomeRequestOrReplyValue> responseStream) {
        return new StreamObserver<Person>() {

            private int numPersons = 0;
            @Override
            public void onNext(Person value) {
                Logit.log("Server received input stream value of: " + value.getName());
                numPersons++;
            }

            @Override
            public void onError(Throwable t) {
                Logit.error(t);
            }


            @Override
            public void onCompleted() {
                Logit.log("Server input stream received on completed: ");
                numPersons++;
                SomeRequestOrReplyValue replyValue = SomeRequestOrReplyValue.newBuilder().
                        setTheVal("Client sent total of " + numPersons + " persons").build();
                responseStream.onNext(replyValue);
                responseStream.onCompleted();
            }

        };
    }


    public static void log(String s){
        System.out.println(s);
    }
}
