package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;

public class AddressService implements IAddressService {


    @Override
    public SomeRequestOrReplyValue handlePerson(Person person){
        log("handlePerson");
        log(person.toString());
        return SomeRequestOrReplyValue.newBuilder().setTheVal("handled " + person.getName()).build();
    }

    @Override
    public SomeRequestOrReplyValue handleAddress(AddressBook book){
        log("handleAddress");
        log(book.toString());
        return SomeRequestOrReplyValue.newBuilder().setTheVal("got an address ").build();

    }

    @Override
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, MPStreamObserver<Person> personStream)
    throws Exception{
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
    public MPStreamObserver<Person> clientStreamPersons(MPStreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {
        return new MPStreamObserver<Person>() {

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
