package exp.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkQueueExecutor extends ThreadPoolExecutor {


    public WorkQueueExecutor(){
        super(1,50,1,TimeUnit.SECONDS,new WorkQueue(null,null,null));
    }
    public WorkQueueExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, WorkQueue workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, new ThreadFactory() {
            AtomicInteger atomicInteger = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"work-queue-runner-"+atomicInteger.getAndIncrement());
            }
        });
    }

    public WorkQueue getWorkQueue(){
        return (WorkQueue) getQueue();
    }
}
