package io.mgrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CreditHandler {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private volatile int credit;

    private final Lock lock = new ReentrantLock();
    private final Condition creditAvailable = lock.newCondition();

    public CreditHandler(int credit) {
        this.credit = credit;
    }

    public void addCredit(int credit) {
        lock.lock();
        this.credit += credit;
        log.info("Adding credit {}. Total = {}", credit, this.credit);
        creditAvailable.signal();
        lock.unlock();
    }

    public void waitForCredit() {
        if(credit != 0){
            credit = credit - 1;
            return;
        }
        try {
            lock.lock();
            //Time out after 3 minutes because there may be some problem with the source not sending credit
            //or an exception during the call. The worst that can happen here is that we will send on more
            //messages to the target but it has limit on the queue size of its MessageProcessor that will
            //cut in and fail the call
            log.debug("Waiting for credit");
            creditAvailable.await(3, TimeUnit.MINUTES);
            credit = credit - 1;
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for credit", e);
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }


}
