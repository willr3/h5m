package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.entity.Work;
import io.hyperfoil.tools.h5m.queue.WorkQueueExecutor;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.util.List;

@ApplicationScoped
public class WorkService {

    @Inject
    EntityManager em;

    @Inject
    @Named("workExecutor")
    WorkQueueExecutor workExecutor;

    @Inject
    NodeService nodeService;

    @Inject
    ValueService valueService;

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

    //does work A depend on work B?
    @Transactional
    public boolean dependsOn(Work a, Work b){
        return dependsOn(a,b,false);
    }
    public boolean dependsOn(Work a, Work b,boolean cumulative){
        if (a.id == null || b.id == null){
            return a.dependsOn(b);
        }
        boolean dependentValue = em.createNativeQuery("""
            select 1 from value_edge 
                 where child_id in (select value_id from work_values where work_id = :workAId) 
                   and parent_id in (select value_id from work_values where work_id = :workBId)
                   and child_id != parent_id 
        """)
                .setParameter("workAId",a.id)
                .setParameter("workBId",b.id)
                .getResultList().size() > 0;
        if (dependentValue){
            return true;
        }
        boolean activeNode = a.activeNode!=null && b.activeNode != null && nodeService.dependsOn(a.activeNode,b.activeNode);

        boolean sameValue = em.createNativeQuery("""
            select 1 from work_values a join work_values b on a.value_id = b.value_id where a.work_id = :workAId and b.work_id = :workBId 
        """)
                .setParameter("workAId",a.id)
                .setParameter("workBId",b.id)
                .getResultList().size() > 0;

        boolean emptyValues = em.createNativeQuery("select 1 from work_values where work_id = :workId").setParameter("workId",a.id)
                .getResultList().size() == 0;


        return activeNode && (sameValue || cumulative || a.cumulative || emptyValues);
    }
}
