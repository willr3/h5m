package io.hyperfoil.tools.h5m.provided;

import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.hyperfoil.tools.h5m.svc.NodeGroupService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class ExecutorConfiguration {

    @Inject
    MeterRegistry registry; // Injected by Quarkus Micrometer


    @Inject
    WorkService workService;
    @Inject
    NodeGroupService nodeGroupService;
    @Inject
    NodeService nodeService;
    @Inject
    ValueService valueService;

    @Inject @ConfigProperty(name = "h5m.work.maximumPoolSize",defaultValue = "1")
    int maximumPoolSize;
    @Inject @ConfigProperty(name = "h5m.work.keepAlive",defaultValue = "10")
    int keepAlive;
    @Inject @ConfigProperty(name = "h5m.work.KeepAliveUnit",defaultValue = "seconds")
    String keepAliveUnit;

    private TimeUnit convertTimeUnit(String input){
        if (input != null && !input.isEmpty()) {
            try {
                return TimeUnit.valueOf(input.toUpperCase());
            } catch (IllegalArgumentException e) {
                //TODO log unknown input
            }
        }
        return TimeUnit.SECONDS;
    }

    @Alternative
    @Produces
    @ApplicationScoped
    @Priority(9997)
    @Named("workExecutor")
    public WorkQueueExecutor initDatasource(/*CommandLine.ParseResult parseResult*/) throws SQLException {

        WorkQueue workQueue = new WorkQueue(nodeService,valueService,workService);
        WorkQueueExecutor rtrn = new WorkQueueExecutor(
                maximumPoolSize,
                maximumPoolSize,
                keepAlive,
                convertTimeUnit(keepAliveUnit),
                workQueue
        );
        rtrn.allowCoreThreadTimeOut(false);
        rtrn.prestartAllCoreThreads();

        ExecutorServiceMetrics serviceMetrics = new ExecutorServiceMetrics(rtrn,"workExecutor",null);
        serviceMetrics.bindTo(registry);
        return rtrn;
    }

    public void cleanup(@Disposes ThreadPoolExecutor executor) {
        executor.shutdown();
    }
}