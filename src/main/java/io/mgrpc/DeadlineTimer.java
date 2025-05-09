package io.mgrpc;

import io.grpc.Deadline;

import javax.annotation.Nullable;
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


    private final long remainingMillis;
    private final DeadlineTimerListener listener;



    DeadlineTimer(long remainingMillis, DeadlineTimerListener listener) {
        this.remainingMillis = remainingMillis;
        this.listener = listener;
    }


    public static ScheduledFuture<?> start(Deadline deadline, DeadlineTimerListener listener) {
        long remainingMillis = deadline.timeRemaining(TimeUnit.MILLISECONDS);
        return TimerService.get().schedule(
                new DeadlineTimer(remainingMillis, listener), remainingMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        float seconds = ((float)remainingMillis/(float)1000);
        listener.onDeadline(String.format("deadline exceeded after %.3fs", seconds));
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



