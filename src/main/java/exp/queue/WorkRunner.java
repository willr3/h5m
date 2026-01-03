package exp.queue;

import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.entity.node.JsNode;
import exp.entity.node.SqlJsonpathNode;
import exp.svc.*;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.transaction.TransactionScoped;
import jakarta.transaction.Transactional;
import org.hibernate.Session;

import java.util.*;
import java.util.stream.Collectors;

public class WorkRunner implements Runnable {

    public static final int RETRY_LIMIT = 0;

    NodeService nodeService;

    ValueService valueService;

    WorkService workService;

    @Inject
    NodeGroupService nodeGroupService;

    //@Inject
    EntityManagerFactory emf;


    Work work;
    private WorkQueue workQueue;

    private Runnable then;

    public WorkRunner(Work work){
        this.work = work;
        //this.workQueue = CDI.current().select(WorkQueue.class).get();
        this.nodeService = CDI.current().select(NodeService.class).get();
        this.valueService = CDI.current().select(ValueService.class).get();
        this.workService = CDI.current().select(WorkService.class).get();
    }

    public WorkRunner(Work work,WorkQueue workQueue,NodeService nodeService,ValueService valueService,WorkService workService){
        this.work = work;
        this.workQueue = workQueue;
        this.nodeService = nodeService;
        this.valueService = valueService;
        this.workService = workService;
    }

    @Override
    public String toString(){
        return "WorkRunner work.id="+work.id+" work.node.id="+work.activeNode.id+" work.node.name="+work.activeNode.name;
    }

    public WorkRunner then(Runnable then){
        this.then = then;
        return this;
    }

    @Transactional
    @Override
    public void run() {
        try {
            if(work.activeNode==null || work.sourceValues == null || work.sourceValues.isEmpty()){
                //error conditions?
                //work.activeNode == null is not yet a valid condition but it could be for post processing tasks?
            }
            //looping over values works for Jq / Js nodes but what about cross test comparison
            //calculateValue should probably accept all sourceValues and leave it to the node function to decide
            List<Value> calculated = nodeService.calculateValues(work.activeNode,work.sourceValues);

            //if the activeNode is a sqlpath then the entity is already persisted
            boolean allPersisted = calculated.stream().allMatch(v->v.getId()!=null);
            List<Value> newOrUpdated = new ArrayList<>();
            for(Value v : work.sourceValues) {
                Map<String, Value> descendants = valueService.getDescendantValueByPath(v, work.activeNode);
                for(Iterator<Value> iter =  calculated.iterator(); iter.hasNext();){
                    Value newValue = iter.next();
                    String path = newValue.getPath();
                    if(descendants.containsKey(path)){
                        Value existingValue = descendants.get(path);
                        //existingValue.getId() should not be null because it was fetched from persistence
                        if(existingValue.getId().equals(newValue.getId())) {
                            //if it's the same value we don't have to work with it
                        }else if( newValue.data.equals(existingValue.data)){
                            if(newValue.id != null){
                                valueService.delete(newValue);
                            }
                            iter.remove();//we don't need this new Value
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
            if(then!=null){
                then.run();
            }
            //we need to trigger more calculations? perhaps for a recalculation we do?
            if(!newOrUpdated.isEmpty()){
                if(work.activeNode!=null){
                    List<Node> dependentNodes = nodeService.getDependentNodes(work.activeNode);

                    dependentNodes.forEach(node->{
                        Work newWork = new Work(node,node.sources,work.sourceValues);
                        //workService.create(newWork);
                        boolean added = workQueue.addWork(newWork);
                    });
                }
            }
            //not in the finally so that it only happens if the work succeeds
            //TODO this is throwing TransactionRequiredException
            workService.delete(work);
        }catch( Exception e){
            //TODO how to handle the exception, adding it back to the todo list
            System.err.println("WorkRunner caught: "+e.getMessage()+"\n work="+work);
            e.printStackTrace();
            work.retryCount++;
            if(work.retryCount > RETRY_LIMIT){
                System.err.println("Work exceeded retry limit");
            } else {
                System.err.println("Adding work to retry in queue");
                workQueue.add(this);
            }
        } finally {
            if(work.activeNode!=null){
                workQueue.decrement(work);
            }
        }
    }


}
