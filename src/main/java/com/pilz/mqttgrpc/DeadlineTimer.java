package com.pilz.mqttgrpc;

import io.grpc.Deadline;

import javax.annotation.Nullable;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Note that some of the code around deadline cancellation is copied partly
 * from io.grpc.internal.ClientCallImpl
 */
public class DeadlineTimer implements Runnable{


    public interface DeadlineTimerListener{
        void onDeadline(String deadLineMessage);
    }


    private final long remainingNanos;
    private final DeadlineTimerListener listener;



    DeadlineTimer(long remainingNanos, DeadlineTimerListener listener) {
        this.remainingNanos = remainingNanos;
        this.listener = listener;
    }


    public static ScheduledFuture<?> start(Deadline deadline, DeadlineTimerListener listener) {
        long remainingNanos = deadline.timeRemaining(TimeUnit.NANOSECONDS);
        return TimerService.get().schedule(
                new DeadlineTimer(remainingNanos, listener), remainingNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void run() {
        long seconds = Math.abs(remainingNanos) / TimeUnit.SECONDS.toNanos(1);
        long nanos = Math.abs(remainingNanos) % TimeUnit.SECONDS.toNanos(1);

        StringBuilder buf = new StringBuilder();
        buf.append("deadline exceeded after ");
        if (remainingNanos < 0) {
            buf.append('-');
        }
        buf.append(seconds);
        buf.append(String.format(Locale.US, ".%09d", nanos));
        buf.append("s. ");
        listener.onDeadline(buf.toString());
    }




    public static Deadline min(@Nullable Deadline deadline0, @Nullable Deadline deadline1) {
        if (deadline0 == null) {
            return deadline1;
        }
        if (deadline1 == null) {
            return deadline0;
        }
        return deadline0.minimum(deadline1);
    }


}



