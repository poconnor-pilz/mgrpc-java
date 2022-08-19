package com.pilz.examples.addressbook;

import com.example.tutorial.protos.AddressBook;
import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import com.pilz.mqttgrpc.Logit;
import com.pilz.mqttgrpc.ProtoSender;
import com.pilz.mqttgrpc.ProtoServiceManager;
import com.pilz.mqttgrpc.StreamWaiter;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class TestAddressbook {

    @Test
    public void test1(){
        log.debug("*****************************");
        assertEquals(1,1);
    }

    private MqttAsyncClient makeClient(){
        try {
            final MqttAsyncClient client;
            client = new MqttAsyncClient(
                    "tcp://localhost:1883",
                    MqttAsyncClient.generateClientId(),
                    new MemoryPersistence());
            MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
            client.connect(mqttConnectOptions).waitForCompletion();
            return client;

        } catch (MqttException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void run() throws Exception{
        Person person =
                Person.newBuilder()
                        .setId(1234)
                        .setName("John Doe")
                        .setEmail("jdoe@example.com")
                        .addPhones(
                                Person.PhoneNumber.newBuilder()
                                        .setNumber("555-4321")
                                        .setType(Person.PhoneType.HOME))
                        .build();


        final MqttAsyncClient serverMqttClient = makeClient();
        final MqttAsyncClient clientMqttClient = makeClient();

        //Set up server
        String serviceBaseTopic = "addressservice";
        ProtoServiceManager protoServiceManager = new ProtoServiceManager(serverMqttClient);
        AddressService service = new AddressService();
        AddressSkeleton stub = new AddressSkeleton(service);
        protoServiceManager.subscribeService(serviceBaseTopic, stub);


        //Set up client
        ProtoSender protoSender = new ProtoSender(clientMqttClient, serviceBaseTopic);
        AddressStub proxy = new AddressStub(protoSender);
        StreamWaiter<SomeRequestOrReplyValue> waiter =  new StreamWaiter<>();
        proxy.handlePerson(person, waiter);
        try {
            Logit.log("Received reply: " + waiter.getSingle().toString());
        } catch (Throwable t){
            Logit.error("Failed to get single value");
            Logit.error(t);
        }

        waiter =  new StreamWaiter<>();
        proxy.handleAddress(AddressBook.newBuilder().build(), waiter);
        try {
            Logit.log("Received address reply: " + waiter.getSingle().toString());
        } catch (Throwable t){
            Logit.error("Failed to get single value");
            Logit.error(t);
        }


        final CountDownLatch latch = new CountDownLatch(1);

        SomeRequestOrReplyValue reqVal = SomeRequestOrReplyValue.newBuilder().setTheVal("A request value for the stream").build();
        proxy.serverStreamPersons(reqVal, new StreamObserver<Person>() {
            @Override
            public void onNext(Person value) {
                Logit.log("Received stream value: " + value.getName());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                Logit.log("Received completed stream: ");
                latch.countDown();
            }
        });

        latch.await();


        final CountDownLatch latch2 = new CountDownLatch(1);
        StreamObserver<Person> clientStream = proxy.clientStreamPersons(new StreamObserver<com.example.tutorial.protos.SomeRequestOrReplyValue>() {
            @Override
            public void onNext(com.example.tutorial.protos.SomeRequestOrReplyValue value) {
                Logit.log("Server returned: " + value.getTheVal());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                Logit.log("Server sent completed response after processing client stream");
                latch2.countDown();
            }
        });


        clientStream.onNext(person);
        clientStream.onNext(person);
        clientStream.onCompleted();

        latch2.await();

        serverMqttClient.disconnect();
        serverMqttClient.close();
        clientMqttClient.disconnect();
        clientMqttClient.close();

    }
}
