package exp.svc;

import exp.entity.Work;
import exp.queue.WorkQueueExecutor;
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
}
