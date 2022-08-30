package com.pilz.mqttgrpc;

import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestStreamIterator {
    private static Logger log = LoggerFactory.getLogger(TestStreamIterator.class);

    @Test
    public void testStreamIt(){


        log.debug("test");
        final StreamIterator<String> sit = new StreamIterator<>();
        Thread supplier = new Thread(()->{
            sit.onNext("one");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sit.onNext("two");
            sit.onCompleted();
        });

        supplier.start();

        String all = "";
        while(sit.hasNext()){
            all += sit.next();
        }

        assertEquals("onetwo", all);
    }

    @Test
    public void testWithError(){

        log.debug("test");
        final StreamIterator<String> sit = new StreamIterator<>();
        Thread supplier = new Thread(()->{
            sit.onNext("one");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            sit.onNext("two");
            sit.onError(new Throwable("test inserted failure"));
        });

        supplier.start();

        assertEquals("one", sit.next());
        assertEquals("two", sit.next());

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> sit.next());
        assertEquals("test inserted failure", ex.getStatus().getCause().getMessage());
    }
}
