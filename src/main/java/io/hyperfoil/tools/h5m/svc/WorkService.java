package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.svc.WorkServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.queue.UploadTracker;
import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.quarkus.runtime.StartupEvent;
import io.hyperfoil.tools.h5m.event.ChangeDetectedEvent;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import jakarta.transaction.Transactional;
import jakarta.transaction.TransactionManager;
import io.quarkus.logging.Log;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.hibernate.Hibernate;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class WorkService implements WorkServiceInterface {

    private static final int RETRY_LIMIT = 0;

    @Inject
    EntityManager em;

    @Inject
    TransactionManager tm;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    @Inject
    MeterRegistry registry;

    @Inject
    Event<ChangeDetectedEvent> changeDetectedEvent;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;

    @ConfigProperty(name = "h5m.worker.core", defaultValue = "1")
    int corePoolSize;

    @ConfigProperty(name = "h5m.worker.maxPoolSize", defaultValue = "50")
    int maxPoolSize;

    @ConfigProperty(name = "h5m.worker.keepalive", defaultValue = "PT60S")
    Duration keepAlive;

    private WorkQueueExecutor workExecutor;

    private final ConcurrentHashMap<Long, UploadTracker> trackers = new ConcurrentHashMap<>();

    /**
     * Finds trackers associated with the work's source values.
     * A tracker exists for each root value ID that was registered via createTracked().
     * This derives the association from sourceValues rather than duplicating state on Work.
     */
    private List<UploadTracker> findTrackers(Work work) {
        if (work.sourceValues == null || trackers.isEmpty()) {
            return List.of();
        }
        List<UploadTracker> found = new ArrayList<>();
        for (ValueEntity sv : work.sourceValues) {
            if (sv.id != null) {
                UploadTracker tracker = trackers.get(sv.id);
                if (tracker != null) {
                    found.add(tracker);
                }
            }
        }
        return found;
    }

    private void incrementTrackers(Work work, int count) {
        for (UploadTracker tracker : findTrackers(work)) {
            tracker.increment(count);
        }
    }

    private void decrementTrackers(Work work) {
        for (UploadTracker tracker : findTrackers(work)) {
            tracker.decrement();
        }
    }

    private void failTrackers(Work work, Throwable t) {
        for (UploadTracker tracker : findTrackers(work)) {
            tracker.fail(t);
        }
    }

    @Transactional
    void onStart(@Observes @Priority(1) StartupEvent ev) {
        workExecutor = new WorkQueueExecutor(corePoolSize, maxPoolSize, keepAlive.toSeconds(), TimeUnit.SECONDS, new WorkQueue());
        workExecutor.allowCoreThreadTimeOut(false);
        workExecutor.prestartAllCoreThreads();
        new ExecutorServiceMetrics(workExecutor, "h5mWorkExecutor", null).bindTo(registry);
    }

    @PreDestroy
    void shutdown() {
        if (workExecutor != null) {
            workExecutor.shutdown();
        }
    }

    /**
     * Creates work items and queues them for execution.
     * Work items are NOT persisted to the DB — they exist only in memory.
     * Queue insertion is deferred until the current transaction commits
     * to ensure source values are visible to worker threads.
     */
    @Transactional
    public void create(List<Work> works) {
        WorkQueue workQueue = workExecutor.getWorkQueue();
        List<Work> newWorks = new ArrayList<>();
        for (Work work : works) {
            if (workQueue.hasWork(work)) {
                continue;
            }
            newWorks.add(work);
        }
        if (!newWorks.isEmpty()) {
            List<Work> toQueue = List.copyOf(newWorks);
            for (Work work : toQueue) {
                // Pre-compute ancestor caches while session is open — needed by
                // WorkQueue.sort() → dependsOn() which runs in afterCompletion
                // outside the session. Without this, dependsOn() would try to
                // lazily traverse NodeEntity.sources and fail.
                if (work.activeNodes != null) {
                    work.dependsOn(work);
                }
                // Increment trackers for each work item (before afterCompletion decrement)
                incrementTrackers(work, 1);
            }
            workQueue.incrementDeferred(toQueue.size());
            try {
                tm.getTransaction().registerSynchronization(new Synchronization() {
                    @Override
                    public void beforeCompletion() {}

                    @Override
                    public void afterCompletion(int status) {
                        if (status == Status.STATUS_COMMITTED) {
                            Log.debugf("afterCompletion: queueing %d Work items", toQueue.size());
                            Collection<Work> accepted = workQueue.addWorks(toQueue);
                            // Decrement trackers for rejected duplicates — they were
                            // counted in the increment but will never be executed
                            for (Work work : toQueue) {
                                if (!accepted.contains(work)) {
                                    decrementTrackers(work);
                                }
                            }
                        } else {
                            Log.warnf("Transaction rolled back (status=%d), %d Work items not queued",
                                    status, toQueue.size());
                            // Decrement trackers for rolled-back work
                            for (Work work : toQueue) {
                                decrementTrackers(work);
                            }
                        }
                        workQueue.decrementDeferred(toQueue.size());
                    }
                });
            } catch (Exception e) {
                workQueue.decrementDeferred(toQueue.size());
                // Undo tracker increments
                for (Work work : toQueue) {
                    decrementTrackers(work);
                }
                throw new IllegalStateException(
                        "Failed to register transaction synchronization; refusing to queue before commit", e);
            }
        }
    }

    /**
     * Creates work items with upload completion tracking.
     * Trackers are keyed by root value IDs. The association between Work
     * and trackers is derived from Work.sourceValues — no duplicated state.
     * For cross-upload Work, a single Work can have source values from
     * multiple uploads, and all relevant trackers are updated automatically.
     * Returns a CompletableFuture that completes when all trackers complete.
     */
    public CompletableFuture<Void> createTracked(List<Work> works, Set<Long> rootValueIds) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (long rootValueId : rootValueIds) {
            UploadTracker tracker = trackers.computeIfAbsent(rootValueId, UploadTracker::new);
            // Clean up tracker when its future completes
            tracker.getFuture().whenComplete((v, t) -> trackers.remove(rootValueId));
            futures.add(tracker.getFuture());
        }
        create(works);
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns the tracker for the given root value ID, if one exists.
     */
    public Optional<UploadTracker> getTracker(long rootValueId) {
        return Optional.ofNullable(trackers.get(rootValueId));
    }

    @Override
    public boolean isIdle() {
        return workExecutor.getWorkQueue().isIdle();
    }

    @Override
    public boolean terminate(long timeout, TimeUnit unit) throws InterruptedException {
        workExecutor.shutdown();
        return workExecutor.awaitTermination(timeout, unit);
    }

    @Transactional
    public void execute(Work w){
        WorkQueue workQueue = workExecutor.getWorkQueue();
        boolean decrementDeferred = false;
        try {
            // Use source values from the Work. If data is already initialized (normal
            // pipeline path via work queue), use it directly to avoid DB round-trip +
            // JSON deserialization. If data is not initialized (e.g., test calling
            // execute() directly with committed entities), reload from DB.
            List<ValueEntity> sourceValues = new ArrayList<>();
            for (ValueEntity sv : w.sourceValues) {
                if (Hibernate.isPropertyInitialized(sv, "data")) {
                    sourceValues.add(sv);
                } else {
                    ValueEntity managed = em.find(ValueEntity.class, sv.id);
                    if (managed != null) {
                        Hibernate.initialize(managed.data);
                        sourceValues.add(managed);
                    }
                }
            }

            // Reload active nodes in this transaction's persistence context —
            // calculateValues() accesses node.sources which is lazy
            Set<NodeEntity> activeNodes = new HashSet<>();
            for (NodeEntity an : w.activeNodes) {
                NodeEntity managed = em.find(NodeEntity.class, an.id);
                if (managed != null) {
                    activeNodes.add(managed);
                }
            }
            if(activeNodes.isEmpty() || sourceValues.isEmpty()){
                // Nothing to process — still need to decrement trackers
                decrementTrackers(w);
                return;
            }

            //looping over values works for Jq / Js nodes but what about cross test comparison
            //calculateValue should probably accept all sourceValues and leave it to the node function to decide
            List<ValueEntity> calculated = new ArrayList<>();
            for(NodeEntity node : activeNodes){
                List<ValueEntity> thisIteration = nodeService.calculateValues(node, sourceValues);
                calculated.addAll(thisIteration);
            }
            if (calculated.isEmpty()) {
                // Node produced no values (e.g., JQ expression didn't match the data).
                // Skip the dedup loop and cascade — no DB queries needed.
                return;
            }
            List<ValueEntity> newOrUpdated = new ArrayList<>();
            for(ValueEntity v : sourceValues) {
                for(NodeEntity activeNode : activeNodes){
                    Map<String, ValueEntity> descendants = valueService.getDescendantValueByPath(v, activeNode);
                    for(Iterator<ValueEntity> iter = calculated.iterator(); iter.hasNext();){
                        ValueEntity newValue = iter.next();
                        String path = newValue.getPath();
                        if(descendants.containsKey(path)){
                            ValueEntity existingValue = descendants.get(path);
                            if(existingValue.getId().equals(newValue.getId())) {
                                //if it's the same value we don't have to work with it
                            }else if( newValue.data.equals(existingValue.data)){
                                if(newValue.id != null){
                                    valueService.delete(newValue);
                                }
                                iter.remove();
                            }else{
                                //update the existing value's data via native SQL
                                //(@Immutable entities can't be updated through Hibernate)
                                String updateSql = switch (dbKind) {
                                    case "postgresql" -> "UPDATE value SET data = cast(:data as jsonb) WHERE id = :id";
                                    case "sqlite"     -> "UPDATE value SET data = json(:data) WHERE id = :id";
                                    default           -> "UPDATE value SET data = :data WHERE id = :id";
                                };
                                em.createNativeQuery(updateSql)
                                    .setParameter("data", newValue.data.toJsonString())
                                    .setParameter("id", existingValue.getId())
                                    .executeUpdate();
                                // Evict from 2LC since cached value is now stale
                                em.getEntityManagerFactory().getCache().evict(ValueEntity.class, existingValue.getId());
                                newOrUpdated.add(existingValue);
                            }
                            descendants.remove(path);//remove it so we know what is left over
                        }else{
                            //we need to persist the newValue
                            valueService.create(newValue);
                        }
                    }
                    if(!descendants.isEmpty()){//values that need to be deleted
                        descendants.values().forEach(valueService::delete);
                    }
                }
            }
            newOrUpdated.addAll(calculated);
            if(!newOrUpdated.isEmpty()){
                Set<NodeEntity> createdValues = newOrUpdated.stream().map(v->v.node).collect(Collectors.toSet());
                for(NodeEntity node : createdValues){
                    if(node.isDetection()){
                        List<Long> valueIds = newOrUpdated.stream().map(ValueEntity::getId).toList();
                        long folderId = sourceValues.stream()
                                .filter(v -> v.folder != null)
                                .map(v -> v.folder.id)
                                .findFirst()
                                .orElse(-1L);
                        changeDetectedEvent.fire(new ChangeDetectedEvent(folderId, node.getId(), node.name, valueIds, w.dispatch));
                    }
                    // Cascade work inherits sourceValues and dispatch flag, so tracker
                    // association is derived automatically via findTrackers()
                    List<Work> cascadeWork = nodeService.getDependentNodes(node).stream()
                            .map(n -> {
                                Work cascaded = new Work(n, n.sources, sourceValues);
                                cascaded.dispatch = w.dispatch;
                                return cascaded;
                            })
                            .toList();
                    create(cascadeWork);
                }
            }

            // Defer decrement until after this transaction commits so that
            // isIdle() cannot return true while the DB commit is still in flight.
            if(w.activeNodes != null && !w.activeNodes.isEmpty()){
                decrementDeferred = true;
                tm.getTransaction().registerSynchronization(new Synchronization() {
                    @Override public void beforeCompletion() {}
                    @Override public void afterCompletion(int status) {
                        workQueue.decrement(w);
                        decrementTrackers(w);
                    }
                });
            }
        }catch( Exception e){
            Log.errorf(e, "WorkRunner caught: %s\n work=%s", e.getMessage(), w);
            w.retryCount++;
            if(w.retryCount > RETRY_LIMIT){
                Log.error("Work exceeded retry limit");
                // Fail trackers so CompletableFutures complete exceptionally
                failTrackers(w, e);
            } else {
                Log.info("Adding work to retry in queue");
                workQueue.add(w);
                // Skip decrement in finally — work is re-queued and will be
                // decremented when the retry completes
                decrementDeferred = true;
            }
        } finally {
            if(!decrementDeferred && w.activeNodes != null && !w.activeNodes.isEmpty()){
                workQueue.decrement(w);
                decrementTrackers(w);
            }
        }
    }

}
