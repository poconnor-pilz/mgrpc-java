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

    private final String name; //For debugging

    private final Lock lock = new ReentrantLock();
    private final Condition creditAvailable = lock.newCondition();

    private final Runnable onCreditAvailableHandler;

    public CreditHandler(String name, int credit, Runnable onCreditAvailableHandler) {
        this.name = name;
        this.credit = credit;
        this.onCreditAvailableHandler = onCreditAvailableHandler;
        log.debug("Created new credit handler {} with credit={}", name, credit);
    }

    public CreditHandler(String name, int credit) {
        this(name, credit, null);
    }

    public void addCredit(int credit) {
        lock.lock();
        this.credit += credit;
        log.info("{} added credit={}. Total={}", name, credit, this.credit);
        creditAvailable.signal();
        lock.unlock();
        if (onCreditAvailableHandler != null) {
            onCreditAvailableHandler.run();
        }
    }

    public boolean hasCredit(){
        return this.credit > 0;
    }


    /**
     * Wait for credit to be available. Then decrement the credit by one and return.
     */
    public void waitForAndDecrementCredit() {
        if(credit != 0){
            credit = credit - 1;
            return;
        }
        try {
            lock.lock();
            //Time out after 10 minutes because there may be some problem with the source not sending credit
            //or an exception during the call. The worst that can happen here is that we will send on more
            //messages to the target but it has limit on the queue size of its MessageProcessor that will
            //cut in and fail the call
            log.debug(name + " waiting for credit");
            if(!creditAvailable.await(10, TimeUnit.MINUTES)){
                log.error("Timed out waiting for credit");
            }
            credit = credit - 1;
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for credit", e);
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }


}
