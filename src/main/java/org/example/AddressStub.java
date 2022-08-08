package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class AddressStub implements MqttProtoService {

    private final IAddressService addressService;

    public AddressStub(IAddressService addressService) {
        this.addressService = addressService;
    }

    @Override
    public MqttProtoBufferObserver onProtoRequest(String method, ByteString params, MqttProtoBufferObserver replyListener) throws Exception{
        switch (method) {
            case IAddressService.METHOD_PERSON: {

                replyListener.onLast(this.addressService.handlePerson(Person.parseFrom(params)).toByteString());
                break;
            }
            case IAddressService.METHOD_ADDRESS: {
                replyListener.onLast(this.addressService.handleAddress(AddressBook.parseFrom(params)).toByteString());
                break;
            }
            case IAddressService.METHOD_SERVER_STREAM_PERSONS:{
                this.addressService.serverStreamPersons(SomeRequestOrReplyValue.parseFrom(params), new MqttProtoStreamObserver<Person>() {
                    @Override
                    public void onNext(Person value) {
                        replyListener.onNext(value.toByteString());
                    }

                    @Override
                    public void onError(Throwable t) {
                        //TODO: do something like this
                        Logit.error("Error");
                        //replyListener.onError(new ByteString(t.getMessage()));
                    }

                    @Override
                    public void onLast(Person value) {
                        replyListener.onLast(value.toByteString());
                    }
                });
                break;
            }
            case IAddressService.METHOD_CLIENT_STREAM_PERSONS: {
                MqttProtoStreamObserver<Person>  inputStream =  this.addressService.clientStreamPersons(new MqttProtoStreamObserver<SomeRequestOrReplyValue>() {
                    @Override
                    public void onNext(SomeRequestOrReplyValue value) {
                        replyListener.onNext(value.toByteString());
                    }

                    @Override
                    public void onError(Throwable t) {
                        //TODO: do something like this
                        Logit.error("Error");
                        //replyListener.onError(new ByteString(t.getMessage()));
                    }

                    @Override
                    public void onLast(SomeRequestOrReplyValue value) {
                        replyListener.onLast(value.toByteString());
                    }
                });

                final MqttProtoBufferObserver protoInputStream = new MqttProtoBufferObserver() {

                    @Override
                    public void onNext(ByteString value) {
                        try {
                            inputStream.onNext(Person.parseFrom(value));
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onLast(ByteString value) {
                        try {
                            inputStream.onLast(Person.parseFrom(value));
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void onError(String error) {
                        //TODO: decode the error properly here when it's not just a string.
                        Logit.error("Error");
                        inputStream.onError(new Throwable(error));
                    }
                };

                return protoInputStream;
            }
            default:
                Logit.log("**************Unrecognised method " + method);
        }

        return null;
    }





}
