package com.pilz.mqttgrpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TimerService {

    private static volatile ScheduledExecutorService executor;
    public static ScheduledExecutorService get(){
        if(executor == null){
            synchronized (TimerService.class){
                if(executor== null){
                    executor = Executors.newScheduledThreadPool(1);
                }
            }
        }
        return executor;
    }
}
