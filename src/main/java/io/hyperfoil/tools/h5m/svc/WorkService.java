package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.svc.WorkServiceInterface;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.quarkus.runtime.StartupEvent;
import io.hyperfoil.tools.h5m.event.ChangeDetectedEvent;
import jakarta.annotation.PreDestroy;
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

    @ConfigProperty(name = "h5m.worker.core", defaultValue = "1")
    int corePoolSize;

    @ConfigProperty(name = "h5m.worker.maxPoolSize", defaultValue = "50")
    int maxPoolSize;

    @ConfigProperty(name = "h5m.worker.keepalive", defaultValue = "PT60S")
    Duration keepAlive;

    private WorkQueueExecutor workExecutor;

    @Transactional
    void onStart(@Observes StartupEvent ev) {
        workExecutor = new WorkQueueExecutor(corePoolSize, maxPoolSize, keepAlive.toSeconds(), TimeUnit.SECONDS, new WorkQueue());
        workExecutor.allowCoreThreadTimeOut(false);
        workExecutor.prestartAllCoreThreads();
        new ExecutorServiceMetrics(workExecutor, "h5mWorkExecutor", null).bindTo(registry);

        //resumes unfinished work from previous execution
        workExecutor.getWorkQueue().addWorks(Work.listAll());
    }

    @PreDestroy
    void shutdown() {
        if (workExecutor != null) {
            workExecutor.shutdown();
        }
    }
    @Transactional
    public void create(List<Work> works) {
        WorkQueue workQueue = workExecutor.getWorkQueue();
        List<Work> newWorks = new ArrayList<>();
        for (Work work : works) {
            if (workQueue.hasWork(work)) {
                continue;
            }
            if (!work.isPersistent()) {
                work.id = null;
                Work merged = em.merge(work);
                em.flush();
                work.id = merged.id;
            }
            newWorks.add(work);
        }
        if (!newWorks.isEmpty()) {
            // Defer queue insertion until this transaction commits.
            // Worker threads will only see Work items after the DB row is visible,
            // preventing StaleObjectStateException from the visibility race.
            List<Work> toQueue = List.copyOf(newWorks);
            // Eagerly initialize lazy proxies that WorkQueue.sort() → dependsOn()
            // traverses, since addWorks runs in afterCompletion outside the session.
            for (Work work : toQueue) {
                for (ValueEntity sv : work.sourceValues) {
                    Hibernate.initialize(sv.node);
                    Hibernate.initialize(sv.sources);
                    if (sv.node != null) {
                        // Trigger ancestor cache computation while session is open
                        sv.node.dependsOn(sv.node);
                    }
                }
                if (work.activeNodes != null) {
                    // Trigger ancestor cache computation while session is open
                    work.dependsOn(work);
                }
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
                            workQueue.addWorks(toQueue);
                        } else {
                            Log.warnf("Transaction rolled back (status=%d), %d Work items not queued",
                                    status, toQueue.size());
                        }
                        workQueue.decrementDeferred(toQueue.size());
                    }
                });
            } catch (Exception e) {
                workQueue.decrementDeferred(toQueue.size());
                throw new IllegalStateException(
                        "Failed to register transaction synchronization; refusing to queue before commit", e);
            }
        }
    }

    @Transactional
    public void delete(Work work){
        if(work.id!=null){
            Work.deleteById(work.id);
        }
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
            Work work = em.find(Work.class, w.id);
            if (work == null) {
                Log.warnf("execute: Work id=%d not found in DB (already processed?), skipping", w.id);
                return;
            }
            if(work.activeNodes==null || work.activeNodes.isEmpty() || work.sourceValues == null || work.sourceValues.isEmpty()){
                //error conditions?
                //work.activeNode == null is not yet a validation condition but it could be for post processing tasks?
            }
            // Eagerly initialize lazy-loaded data fields to avoid LazyInitializationException
            // when accessed later in calculateValues (ValueEntity.data is @Basic(fetch = LAZY))
            for (ValueEntity sv : work.sourceValues) {
                Hibernate.initialize(sv.data);
            }
            //looping over values works for Jq / Js nodes but what about cross test comparison
            //calculateValue should probably accept all sourceValues and leave it to the node function to decide
            List<ValueEntity> calculated = new  ArrayList<>();
            for(NodeEntity node : work.activeNodes){
                List<ValueEntity> thisIteration = nodeService.calculateValues(node,work.sourceValues);
                calculated.addAll(thisIteration);
            }
            List<ValueEntity> newOrUpdated = new ArrayList<>();
            for(ValueEntity v : work.sourceValues) {
                for(NodeEntity activeNode : work.activeNodes){
                    Map<String, ValueEntity> descendants = valueService.getDescendantValueByPath(v, activeNode);
                    for(Iterator<ValueEntity> iter = calculated.iterator(); iter.hasNext();){
                        ValueEntity newValue = iter.next();
                        String path = newValue.getPath();
                        if(descendants.containsKey(path)){
                            ValueEntity existingValue = descendants.get(path);
                            //existingValue.getId() should not be null because it was fetched from persistence
                            if(existingValue.getId().equals(newValue.getId())) {
                                //if it's the same value we don't have to work with it
                            }else if( newValue.data.equals(existingValue.data)){
                                if(newValue.id != null){
                                    valueService.delete(newValue);
                                }
                                iter.remove();//we don't need this new ValueEntity
                            }else{
                                //update the new value
                                existingValue.data = newValue.data;
                                newOrUpdated.add(existingValue);
                                //should this update the created_at
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
                        long folderId = work.sourceValues.stream()
                                .filter(v -> v.folder != null)
                                .map(v -> v.folder.id)
                                .findFirst()
                                .orElse(-1L);
                        changeDetectedEvent.fire(new ChangeDetectedEvent(folderId, node.getId(), node.name, valueIds, true));

                    }
                    //we need to trigger more calculations? perhaps for a recalculation we do?
                    create(nodeService.getDependentNodes(node).stream().map(n -> new Work(n, n.sources, work.sourceValues)).toList());
                }
            }

            //not in the finally so that it only happens if the work succeeds
            delete(work);

            // Defer decrement until after this transaction commits so that
            // isIdle() cannot return true while the DB commit is still in flight.
            if(w.activeNodes!=null && !w.activeNodes.isEmpty()){
                decrementDeferred = true;
                tm.getTransaction().registerSynchronization(new Synchronization() {
                    @Override public void beforeCompletion() {}
                    @Override public void afterCompletion(int status) {
                        workQueue.decrement(w);
                    }
                });
            }
        }catch( Exception e){
            //TODO how to handle the exception, adding it back to the todo list
            System.err.println("WorkRunner caught: "+e.getMessage()+"\n work="+w);
            e.printStackTrace();
            w.retryCount++;
            if(w.retryCount > RETRY_LIMIT){
                System.err.println("Work exceeded retry limit");
            } else {
                System.err.println("Adding work to retry in queue");
                workQueue.add(w);
            }
        } finally {
            // Only decrement immediately if we couldn't register the afterCompletion
            // (early return for "not found", or exception before registration)
            if(!decrementDeferred && w.activeNodes!=null && !w.activeNodes.isEmpty()){
                workQueue.decrement(w);
            }
        }
    }

}
