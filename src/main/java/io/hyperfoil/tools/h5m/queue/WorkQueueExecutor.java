package io.hyperfoil.tools.h5m.queue;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkQueueExecutor extends ThreadPoolExecutor {

    private static final AtomicInteger atomicInteger = new AtomicInteger(0);

    private final WorkQueue workQueue;

    public WorkQueueExecutor() {
        this(1, 50, 1, TimeUnit.SECONDS, new WorkQueue());
    }

    public WorkQueueExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, WorkQueue workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, r -> new Thread(r, "work-queue-runner-" + atomicInteger.getAndIncrement()));
        this.workQueue = workQueue;
    }

    public WorkQueue getWorkQueue() {
        return workQueue;
    }
}
