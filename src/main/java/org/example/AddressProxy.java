package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;

public class AddressProxy implements IAddressService{

    final ProtoSender protoSender;

    public AddressProxy(ProtoSender protoSender) {
        this.protoSender = protoSender;
    }

    @Override
    public SomeRequestOrReplyValue handlePerson(Person person) throws Exception {

        ByteString reply = protoSender.blockingSend(IAddressService.METHOD_PERSON, person);
        return SomeRequestOrReplyValue.parseFrom(reply);

    }

    @Override
    public SomeRequestOrReplyValue handleAddress(AddressBook book) throws Exception {
        ByteString reply = protoSender.blockingSend(IAddressService.METHOD_ADDRESS, book);
        return SomeRequestOrReplyValue.parseFrom(reply);
    }

    @Override
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, MPStreamObserver<Person> responseStream) throws Exception {
        protoSender.sendSingleRequest(IAddressService.METHOD_SERVER_STREAM_PERSONS, requestVal,
                new BufferToStreamObserver<>(Person.parser(), responseStream));
    }




    @Override
    public MPStreamObserver<Person> clientStreamPersons(MPStreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {

        final String streamId = Base64Utils.randomId();

        protoSender.sendInputStreamRequest(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId,
                new BufferToStreamObserver<>(SomeRequestOrReplyValue.parser(), responseStream));

        return new StreamObserverToSender<>(protoSender, IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId);

    }
}
