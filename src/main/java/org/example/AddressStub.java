package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;


public class AddressStub implements MqttProtoService {

    private final IAddressService addressService;

    public AddressStub(IAddressService addressService) {
        this.addressService = addressService;
    }

    @Override
    public MPBufferObserver onProtoRequest(String method, ByteString params, MPBufferObserver replyListener) throws Exception{
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
                this.addressService.serverStreamPersons(SomeRequestOrReplyValue.parseFrom(params),
                        new StreamObserverToBufferObserver<>(replyListener));
                break;
            }
            case IAddressService.METHOD_CLIENT_STREAM_PERSONS: {
                MPStreamObserver<Person> inputStream =  this.addressService.clientStreamPersons(new StreamObserverToBufferObserver<>(replyListener));
                final MPBufferObserver protoInputStream = new BufferObserverToStreamObserver<Person>(Person.parser(), inputStream);
                return protoInputStream;
            }
            default:
                Logit.log("**************Unrecognised method " + method);
        }

        return null;
    }





}
