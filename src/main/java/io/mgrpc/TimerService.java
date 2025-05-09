package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerService {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static volatile ScheduledExecutorService executor;
    public static ScheduledExecutorService get(){
        if(executor == null){
            synchronized (TimerService.class){
                if(executor== null){
                    executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            final String name = "mgrpc-timer-" + threadNumber.getAndIncrement();
                            Thread t = new Thread(r, name);
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return executor;
    }
}
