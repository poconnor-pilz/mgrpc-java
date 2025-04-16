package io.mgrpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TimerService {

    private static volatile ScheduledExecutorService executor;
    public static ScheduledExecutorService get(){
        if(executor == null){
            synchronized (TimerService.class){
                if(executor== null){
                    executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                        private final AtomicInteger threadNumber = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "mgrpc-timer-" + threadNumber.getAndIncrement());
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
