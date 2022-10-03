package com.pilz.mqttgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;

public class TryOrdering {

    private static Logger log = LoggerFactory.getLogger(TryOrdering.class);


    /**
     * Holds an unordered list of recent counter values
     * Counter values are assumed not to be zero (i.e. start at 1)
     */
    private class Recents{

        private static final int DEFAULT_SIZE = 20;
        private final int size;
        private final int[] recents;
        private int index = 0;

        private Recents(int size) {
            this.size = size;
            recents = new int[size];
        }

        private Recents(){
            this(DEFAULT_SIZE);
        }

        public void add(int counter){
            recents[index] = counter;
            index++;
            //Just keep going around the recents buffer and overwriting with the next value
            //We don't care what order the recents are as long as we have them
            if(index == size){
                index = 0;
            }
        }

        public boolean contains(int counter){
            for(int i = 0; i < size; i++){
                if(recents[i] == 0){
                    //buffer not fully populated with recents yet
                    return false;
                }
                if(recents[i] == counter){
                    return true;
                }
            }
            return false;
        }

    }

    private int previous = 0;
    private boolean buffering = false;

    private boolean first = true;

    private ArrayList<Integer> buffer;

    private final Recents recents = new Recents(Recents.DEFAULT_SIZE);

    public static void main(String[] args){
        new TryOrdering().run();
    }

    public void run(){
        send(1, 2, 3, 4, 3, 5, 6, 7, 10, 8, 5, 9, 11, 12, 13, 14, 16, 15, 17);
    }

    public void send(int... values){
        for (int value: values) {
            receive(value);
        }
    }

    public void receive(Integer value){

        //First ignore any duplicates
        if(recents.contains(value)){
            log.warn("Duplicate value {} received. Ignoring.", value);
            return;
        }
        recents.add(value);

        if(first){
            first = false;
            previous = value;
            sendOn(value);
            return;
        }

        //TODO: Mqtt guarantees delivery of the message at some point. But we should have some sort of limit here
        //If the out of order message does not arrive after a certain number of messages then we should fail
        //We could say that if it doesn't arrive after a certain time then we should fail but
        //let the client do timeouts where possible as some clients may not care as long as they get the message some time
        //Also we should consider the visu use case where we always want to get the latest value so we would accept
        //something that is more than one greater than last and then reject anything that is less than last.
        //But for most service oriented things which are streaming values then the order of the stream will be important
        //This could be configured as part of the MqttGrpcClient
        if(buffering) {
            buffer.add(value);
            Collections.sort(buffer);
            int lastlocal = buffer.get(0);
            boolean contiguous = true;
            for (int i = 1; i < buffer.size(); i++) {
                Integer next = buffer.get(i);
                if (next - lastlocal > 1) {
                    contiguous = false;
                    break;
                }
                lastlocal = next;
            }
            if (contiguous) {
                for (int i = 1; i < buffer.size(); i++) {
                    sendOn(buffer.get(i));
                }
                buffering = false;
                log.debug("Buffering finished");
            }
        } else if (value != previous + 1) {
            log.warn("Out of order value {} received. Start buffering and sorting.", value);
            buffer = new ArrayList<>();
            //Remember that the first item in the buffer will need to be ignored
            //when sending on as is the last value and was already processed
            buffer.add(previous);
            buffer.add(value);
            buffering = true;
            return;
       } else {
            sendOn(value);
        }

    }

    public void sendOn(Integer value){
        previous = value;
        System.out.println(value);
    }
}
