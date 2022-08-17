package org.example;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;


public class AddressSkeleton implements MqttProtoService {

    private final IAddressService addressService;

    public AddressSkeleton(IAddressService addressService) {
        this.addressService = addressService;
    }

    @Override
    public BufferObserver onProtoRequest(String method, ByteString params, BufferObserver serverBufferObserver) throws Exception{
        switch (method) {
            case IAddressService.METHOD_PERSON: {
                addressService.handlePerson(Person.parseFrom(params), new SingleToBufferObserver<>(serverBufferObserver));
                break;
            }
            case IAddressService.METHOD_ADDRESS: {
                addressService.handleAddress(AddressBook.parseFrom(params), new StreamToBufferObserver<>(serverBufferObserver));
                break;
            }
            case IAddressService.METHOD_SERVER_STREAM_PERSONS:{
                addressService.serverStreamPersons(SomeRequestOrReplyValue.parseFrom(params),
                        new StreamToBufferObserver<>(serverBufferObserver));
                break;
            }
            case IAddressService.METHOD_CLIENT_STREAM_PERSONS: {
                StreamObserver<Person> inputStream =  addressService.clientStreamPersons(new StreamToBufferObserver<>(serverBufferObserver));
                return new BufferToStreamObserver<>(Person.parser(), inputStream);
            }
            default:
                Logit.log("**************Unrecognised method " + method);
        }

        return null;
    }





}
