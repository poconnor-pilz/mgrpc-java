package com.pilz.examples.addressbook;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.google.protobuf.ByteString;
import com.pilz.mqttgrpc.*;
import io.grpc.stub.StreamObserver;


public class AddressSkeleton implements Skeleton     {

    private final IAddressService addressService;

    public AddressSkeleton(IAddressService addressService) {
        this.addressService = addressService;
    }

    @Override
    public BufferObserver onRequest(String method, ByteString request, BufferObserver responseObserver) throws Exception{
        switch (method) {
            case IAddressService.PERSON: {
                addressService.handlePerson(Person.parseFrom(request), new SingleToStreamObserver<>(responseObserver));
                break;
            }
            case IAddressService.ADDRESS: {
                addressService.handleAddress(AddressBook.parseFrom(request), new BufferToStreamObserver<>(responseObserver));
                break;
            }
            case IAddressService.SERVER_STREAM_PERSONS:{
                addressService.serverStreamPersons(SomeRequestOrReplyValue.parseFrom(request),
                        new BufferToStreamObserver<>(responseObserver));
                break;
            }
            case IAddressService.CLIENT_STREAM_PERSONS: {
                StreamObserver<Person> inputStream =  addressService.clientStreamPersons(new BufferToStreamObserver<>(responseObserver));
                return new StreamToBufferObserver<>(Person.parser(), inputStream);
            }
            default:
                Logit.log("**************Unrecognised method " + method);
        }

        return null;
    }





}
