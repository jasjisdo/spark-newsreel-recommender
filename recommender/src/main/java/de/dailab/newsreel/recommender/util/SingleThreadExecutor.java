package de.dailab.newsreel.recommender.util;

import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;

/**
 * Created by jens on 19.01.16.
 */
public class SingleThreadExecutor {

    private static final int MAX_QUEUE_SIZE = 1;
    private static final int MAX_SLEEP_TIME = 1000;
    private static final int MIN_SLEEP_TIME = 100;

    private static final Logger log = Logger.getLogger(SingleThreadExecutor.class);
    private final ConcurrentLinkedQueue<Thread> q = new ConcurrentLinkedQueue<>();
    private final Random r = new Random(System.currentTimeMillis());

    public SingleThreadExecutor() {
        new Thread() {
            @Override
            public void run() {
                while (true)
                    execution();
            }
        }.start();
    }

    private void execution() {
        try {
            Thread.currentThread().sleep(sleepTime());
        } catch (InterruptedException e) {
            log.warn("Worker Thread interrupted while being idle",e);
        }
        if (q.isEmpty()) return;
        Thread current = q.poll();
        current.run();
        return;
    }

    private long sleepTime() {
        Double range = r.nextDouble() * (MAX_SLEEP_TIME - MIN_SLEEP_TIME) + MIN_SLEEP_TIME;
        return range.longValue();
    }

    public void submit(Thread thread) {
        if (q.size() >= MAX_QUEUE_SIZE) return;
        q.add(thread);
    }
}
