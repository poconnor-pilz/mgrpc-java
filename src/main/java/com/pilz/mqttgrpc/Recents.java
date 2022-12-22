package com.pilz.mqttgrpc;

import java.util.Arrays;

/**
 * Holds an unordered list of recent counter values
 * Counter values are assumed to be >=0
 */
class Recents {

    public static final int DEFAULT_SIZE = 20;
    private static final int EMPTY_VALUE = -1;
    private final int size;
    private final int[] recents;
    private int index = 0;

    public Recents(int size) {
        this.size = size;
        recents = new int[size];
        Arrays.fill(recents, EMPTY_VALUE);
    }

    public Recents() {
        this(DEFAULT_SIZE);
    }

    public void add(int counter) {
        recents[index] = counter;
        index++;
        //Just keep going around the recents buffer and overwriting with the next value
        //We don't care what order the recents are as long as we have them
        if (index == size) {
            index = 0;
        }
    }

    public boolean contains(int counter) {
        for (int i = 0; i < size; i++) {
            if (recents[i] == EMPTY_VALUE) {
                //buffer not fully populated with recents yet
                return false;
            }
            if (recents[i] == counter) {
                return true;
            }
        }
        return false;
    }

}
