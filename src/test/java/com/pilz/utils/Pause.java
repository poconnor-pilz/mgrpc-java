package com.pilz.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pause {

    private static final Logger log = LoggerFactory.getLogger(Pause.class);
    public static void pause(long millis){
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("Interrupted during pause ", e);
        }
    }
}
