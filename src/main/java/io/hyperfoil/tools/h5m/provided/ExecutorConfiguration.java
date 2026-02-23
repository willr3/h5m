package io.hyperfoil.tools.h5m.provided;

import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class ExecutorConfiguration {

    @Inject
    MeterRegistry registry; // Injected by Quarkus Micrometer

    @Produces
    @Dependent
    public WorkQueueExecutor create(
            @ConfigProperty(name = "h5m.worker.core", defaultValue = "1") int core,
            @ConfigProperty(name = "h5m.worker.maxPoolSize", defaultValue = "50") int max,
            @ConfigProperty(name = "h5m.worker.keepalive", defaultValue = "PT60S") Duration keepAlive) {
        WorkQueueExecutor rtrn = new WorkQueueExecutor(max, max, keepAlive.toSeconds(), TimeUnit.SECONDS, new WorkQueue());
        rtrn.allowCoreThreadTimeOut(false);
        rtrn.prestartAllCoreThreads();

        new ExecutorServiceMetrics(rtrn,"h5mWorkExecutor",null).bindTo(registry);
        return rtrn;
    }

    public void cleanup(@Disposes WorkQueueExecutor executor) {
        executor.shutdown();
    }
}