package io.hyperfoil.tools.h5m.provided;

import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class ExecutorConfigurationTest {

    @Inject
    @Named("workExecutor")
    WorkQueueExecutor executor;

    @Test
    public void initialize_with_inject() {
        WorkQueue workQueue = executor.getWorkQueue();
        assertNotNull(workQueue,"queue should not be null");
    }
}
