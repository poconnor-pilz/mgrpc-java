package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

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
    public void serverStreamPersons(SomeRequestOrReplyValue requestVal, MqttProtoStreamObserver<Person> responseStream) throws Exception {
        protoSender.sendSingleRequest(IAddressService.METHOD_SERVER_STREAM_PERSONS, requestVal, new MqttProtoBufferObserver() {
            @Override
            public void onNext(ByteString value) {
                try {
                    responseStream.onNext(Person.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onLast(ByteString value) {
                try {
                    responseStream.onLast(Person.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(String error) {
                //TODO: Handle error
                System.out.println("********Error******** " + error);
            }
        });
    }

    @Override
    public MqttProtoStreamObserver<Person> clientStreamPersons(MqttProtoStreamObserver<SomeRequestOrReplyValue> responseStream) throws Exception {

        final String streamId = Base64Utils.randomId();

        protoSender.sendInputStreamRequest(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId, new MqttProtoBufferObserver() {
            @Override
            public void onNext(ByteString value) {
                try {
                    responseStream.onNext(SomeRequestOrReplyValue.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onLast(ByteString value) {
                try {
                    responseStream.onLast(SomeRequestOrReplyValue.parseFrom(value));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(String error) {
                //TODO: Handle error
                System.out.println("********Error******** " + error);
            }
        });

        return new MqttProtoStreamObserver<Person>() {
            @Override
            public void onNext(Person value) {
                try {
                    protoSender.sendNextStreamValue(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId, value);
                } catch (Exception e) {
                    //TODO: handle error
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable t) {
                try {
                    //TODO encode the exeption as a protobuf
                    protoSender.sendErrorToStream(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId, t.getMessage());
                } catch (Exception e) {
                    //TODO: handle error
                    e.printStackTrace();
                }

            }

            @Override
            public void onLast(Person value) {
                try {
                    protoSender.sendLastStreamValue(IAddressService.METHOD_CLIENT_STREAM_PERSONS, streamId, value);
                } catch (Exception e) {
                    //TODO: handle error
                    e.printStackTrace();
                }
            }
        };
    }
}
