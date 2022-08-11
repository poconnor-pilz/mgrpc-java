package org.example;

import com.example.tutorial.protos.Person;
import com.example.tutorial.protos.SomeRequestOrReplyValue;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) throws Exception {
        new Main().run();
    }


    public Main() {

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
        AddressStub stub = new AddressStub(service);
        protoServiceManager.subscribeService(serviceBaseTopic, stub);


        //Set up client
        ProtoSender protoSender = new ProtoSender(clientMqttClient, serviceBaseTopic);
        AddressProxy proxy = new AddressProxy(protoSender);
        SomeRequestOrReplyValue SomeRequestOrReplyValue = proxy.handlePerson(person);
        Logit.log("Received reply: " + SomeRequestOrReplyValue.toString());


        final CountDownLatch latch = new CountDownLatch(1);

        SomeRequestOrReplyValue reqVal = SomeRequestOrReplyValue.newBuilder().setTheVal("A request value for the stream").build();
        proxy.serverStreamPersons(reqVal, new MPStreamObserver<Person>() {
            @Override
            public void onNext(Person value) {
                Logit.log("Received stream value: " + value.getName());
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onLast(Person value) {
                Logit.log("Received last stream value: " + value.getName());
                latch.countDown();
            }
        });

        latch.await();


        final CountDownLatch latch2 = new CountDownLatch(1);
        MPStreamObserver<Person> inputStream = proxy.clientStreamPersons(new MPStreamObserver<com.example.tutorial.protos.SomeRequestOrReplyValue>() {
            @Override
            public void onNext(com.example.tutorial.protos.SomeRequestOrReplyValue value) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onLast(com.example.tutorial.protos.SomeRequestOrReplyValue value) {
                Logit.log("Server returned: " + value.getTheVal());
                latch2.countDown();
            }
        });


        inputStream.onNext(person);
        inputStream.onLast(person);

        latch2.await();

        serverMqttClient.disconnect();
        serverMqttClient.close();
        clientMqttClient.disconnect();
        clientMqttClient.close();

    }




}