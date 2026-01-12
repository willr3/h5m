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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;

import java.util.List;

@ApplicationScoped
public class WorkService {

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;

    @Inject
    EntityManager em;

    @Inject
    @Named("workExecutor")
    WorkQueueExecutor workExecutor;

    @Inject
    NodeService nodeService;

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
    @Transactional(Transactional.TxType.REQUIRES_NEW)
    public boolean dependsOn(Work a, Work b,boolean cumulative){
        if(a.equals(b)){//added this as a quick hack, need to account or it in other checks
            return false;
        }
        if(a.id == null || b.id == null){
            return a.dependsOn(b);
        }
        try {
            //I don't think this is correct, this is returning true when it shouldn't
            //
            boolean dependentValue = switch(dbKind) {
                case "sqlite" ->
                    em.unwrap(Session.class).createNativeQuery("""
                        with recursive workAValueAncestors(vid,aid) as (
                            select value_id as vid, value_id as aid
                                from work_values where work_id = :workAId -- workA.sourceValues
                            union
                            select a.vid as vid, ve.parent_id as aid
                                from value_edge ve join workAValueAncestors a on a.vid = ve.child_id
                        ),
                        workAValueNodeAncestors(vid,aid,nid) as (
                            select a.vid as vid, a.aid as aid, v.node_id as nid
                                from workAValueAncestors a join value v on a.vid = v.id
                                -- select values that depend on workB.sourceValues
                                where exists (select 1 from work_values where work_id = :workBId and value_id = a.aid)
                            union 
                            select a.vid as vid, a.aid as aid, ne.parent_id as nid
                                from node_edge ne join workAValueNodeAncestors a on a.nid = ne.child_id
                        )
                        select exists (select 1 from workAValueNodeAncestors a where a.nid = :workBActiveNodeId)                    
                    """, Integer.class)
                        .setParameter("workAId", a.id)
                        .setParameter("workBId", b.id)
                        .setParameter("workBActiveNodeId", b.activeNode.getId())
                        .getSingleResult() > 0;
                case "postgresql" ->
                    em.unwrap(Session.class).createNativeQuery("""
                        with recursive workAValueAncestors(vid,aid) as (
                            select value_id as vid, value_id as aid
                                from work_values where work_id = :workAId -- workA.sourceValues
                            union
                            select a.vid as vid, ve.parent_id as aid
                                from value_edge ve join workAValueAncestors a on a.vid = ve.child_id
                        ),
                        workAValueNodeAncestors(vid,aid,nid) as (
                            select a.vid as vid, a.aid as aid, v.node_id as nid
                                from workAValueAncestors a join value v on a.vid = v.id
                                -- select values that depend on workB.sourceValues
                                where exists (select 1 from work_values where work_id = :workBId and value_id = a.aid)
                            union 
                            select a.vid as vid, a.aid as aid, ne.parent_id as nid
                                from node_edge ne join workAValueNodeAncestors a on a.nid = ne.child_id
                        )
                        select exists (select 1 from workAValueNodeAncestors a where a.nid = :workBActiveNodeId)                    
                    """, Boolean.class)
                        .setParameter("workAId", a.id)
                        .setParameter("workBId", b.id)
                        .setParameter("workBActiveNodeId", b.activeNode.getId())
                        .getSingleResult();
                default -> false;
            };

            if (dependentValue) {
                System.out.println(a.id +" "+a.activeNode.operation+ " depends on value from " + b.id+" "+b.activeNode.operation);
                return true;
            }
        }catch(Exception e){
            System.out.println("Exception from a.id="+a.id+" b.id="+b.id+"\n"+e.getMessage());
        }
        boolean activeNode = a.activeNode!=null && b.activeNode != null && nodeService.dependsOn(a.activeNode,b.activeNode);

        boolean sameValue = switch(dbKind){
            case "sqlite" -> em.unwrap(Session.class).createNativeQuery("""
            select exists (select 1 from work_values a join work_values b on a.value_id = b.value_id where a.work_id = :workAId and b.work_id = :workBId and a.work_id != b.work_id)
        """,Integer.class)
                    .setParameter("workAId",a.id)
                    .setParameter("workBId",b.id)
                    .getSingleResult() > 0;
            case "postgresql" -> em.unwrap(Session.class).createNativeQuery("""
            select exists (select 1 from work_values a join work_values b on a.value_id = b.value_id where a.work_id = :workAId and b.work_id = :workBId and a.work_id != b.work_id)
        """,Boolean.class)
                    .setParameter("workAId",a.id)
                    .setParameter("workBId",b.id)
                    .getSingleResult();
            default -> {
                System.out.println("defaulting to false for sameValue");
                yield false;
            }
        };


        boolean emptyValues = switch (dbKind) {
            case "sqlite" -> em.unwrap(Session.class)
                    .createNativeQuery(
                            """
                            select exists (select 1 from work_values where work_id = :workId)
                            """,Integer.class).setParameter("workId",a.id)
                    .getSingleResult() == 0;
            case "postgresql" ->  ! em.unwrap(Session.class)
                    .createNativeQuery("select exists (select 1 from work_values where work_id = :workId)",Boolean.class)
                    .setParameter("workId",a.id)
                    .getSingleResult();
            default -> {
                System.out.println("defaulting to false for emptyValues");
                yield  false;
            }
        };
        return activeNode && (sameValue || cumulative || emptyValues);
    }

}
