package io.mgrpc.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class Pause {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static void pause(long millis){
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.error("Interrupted during pause ", e);
        }
    }
}
