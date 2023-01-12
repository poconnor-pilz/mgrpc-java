package com.pilz.utils;

import java.util.concurrent.Executor;

/**
 * Use this class in tests to make them easier to debug than dealing with the indirections
 * of a thread pool (Even Executors.newSingleThreadExecutor uses a thread pool underneath)
 * Using this class means that we get an in-line call stack.
 */
public class DirectExecutor implements Executor {

    @Override
    public void execute(Runnable command) {
        command.run();
    }
}
