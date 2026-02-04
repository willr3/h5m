package io.hyperfoil.tools.h5m.queue;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkQueueExecutor extends ThreadPoolExecutor {

    private final WorkQueue workQueue;
    public WorkQueueExecutor(){
        this(1,50,1,TimeUnit.SECONDS,new WorkQueue(null,null,null));
    }
    public WorkQueueExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, WorkQueue workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue/*new ArrayBlockingQueue<>(3)*/, new ThreadFactory() {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"work-queue-runner-"+atomicInteger.getAndIncrement());
            }
        });
        this.workQueue = workQueue;
    }
    public WorkQueue getWorkQueue(){
        return workQueue;
    }
}
