package org.example;

import java.util.ArrayList;
import java.util.Collections;

public class TryOrdering {

    private int previous = 0;
    private boolean buffering = false;

    private boolean first = true;

    private ArrayList<Integer> buffer;

    public static void main(String[] args){
        new TryOrdering().run();
    }

    public void run(){
        send(1, 2, 3, 5, 4, 6, 7, 10, 8, 9, 11, 12);
    }

    public void send(int... values){
        for (int value: values) {
            receive(value);
        }
    }

    public void receive(Integer value){

        if(first){
            first = false;
            previous = value;
            return;
        }

        //Duplicates not handled. Dups could arrive any time e.g. 1,2,3,4,2,5 so maybe just keep a recent list
        //And get rid of them using that before running this code here
        if(buffering){
            buffer.add(value);
            Collections.sort(buffer);
            int lastlocal = buffer.get(0);
            boolean contiguous = true;
            for(int i = 1; i < buffer.size(); i++){
                Integer next = buffer.get(i);
                if(next - lastlocal > 1 ){
                    contiguous = false;
                    break;
                }
                lastlocal = next;
            }
            if(contiguous){
                for(int i = 1; i < buffer.size(); i++){
                    sendOn(buffer.get(i));
                }
                buffering = false;
            }
        } else if (value != previous + 1) {
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
