package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.h5m.queue.WorkQueue;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class WorkService {

    private static final int RETRY_LIMIT = 0;

    @Inject
    EntityManager em;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

    @Inject
    @Named("workExecutor")
    WorkQueueExecutor workExecutor;

    //resumes unfinished work from previous execution
    @Transactional
    void onStart(@Observes StartupEvent ev) {
        List<Work> all = loadAll();
        workExecutor.getWorkQueue().addWorks(all);
    }


    @Transactional
    public long create(Work work){
        if(!work.isPersistent()){
            work.id = null;
            Work merged = em.merge(work);
            em.flush();
            work.id = merged.id;
            return merged.id;
        }
        return work.id;
    }

    @Transactional
    public List<Work> loadAll(){
        return Work.listAll();
    }

    @Transactional
    public void delete(Work work){
        if(work.id!=null){
            Work.deleteById(work.id);
        }
    }

    @Transactional
    public void execute(Work w){
        WorkQueue workQueue = workExecutor.getWorkQueue();
        try {
            Work work = em.merge(w);
            if(work.activeNode==null || work.sourceValues == null || work.sourceValues.isEmpty()){
                //error conditions?
                //work.activeNode == null is not yet a validation condition but it could be for post processing tasks?
            }
            //looping over values works for Jq / Js nodes but what about cross test comparison
            //calculateValue should probably accept all sourceValues and leave it to the node function to decide
            List<ValueEntity> calculated = nodeService.calculateValues(work.activeNode,work.sourceValues);

            //if the activeNode is a sqlpath then the entity is already persisted
            boolean allPersisted = calculated.stream().allMatch(v->v.getId()!=null);
            List<ValueEntity> newOrUpdated = new ArrayList<>();
            for(ValueEntity v : work.sourceValues) {
                Map<String, ValueEntity> descendants = valueService.getDescendantValueByPath(v, work.activeNode);
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
            newOrUpdated.addAll(calculated);

            //we need to trigger more calculations? perhaps for a recalculation we do?
            if(!newOrUpdated.isEmpty()){
                if(work.activeNode!=null){
                    List<NodeEntity> dependentNodes = nodeService.getDependentNodes(work.activeNode);

                    dependentNodes.forEach(node->{
                        Work newWork = new Work(node,node.sources,work.sourceValues);
                        create(newWork);
                        boolean added = workQueue.addWork(newWork);
                    });
                }
            }
            //not in the finally so that it only happens if the work succeeds
            delete(work);
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
            if(w.activeNode!=null){
                workQueue.decrement(w);
            }
        }
    }

}
