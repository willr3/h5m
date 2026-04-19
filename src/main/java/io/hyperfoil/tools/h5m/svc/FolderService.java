package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.hibernate.query.NativeQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class FolderService implements FolderServiceInterface {

    @Inject
    EntityManager em;
    @Inject
    ValueService valueService;
    @Inject
    WorkService workService;

    @Inject
    AuthorizationService authService;

    @Inject
    ApiMapper apiMapper;

    @Override
    @Transactional
    public long create(String name){
        FolderEntity entity = new FolderEntity();
        entity.name = name;
        entity.group = new NodeGroupEntity(name); //TODO do we auto-create a nodeGroup?
        FolderEntity.persist(entity);
        return entity.id;
    }

    @Transactional
    public long create(String name, String teamName) {
        Team team = Team.find("name", teamName).firstResult();
        if (team == null) {
            throw new IllegalArgumentException("Team not found: " + teamName);
        }
        long id = create(name);
        FolderEntity entity = FolderEntity.findById(id);
        entity.team = team;
        return id;
    }

    @Transactional
    public FolderEntity read(long id){
        return FolderEntity.findById(id);
    }

    @Override
    @Transactional
    public Folder byName(String name){
        return FolderEntity.find("name",name).project(Folder.class).firstResult();
    }
    @Transactional
    public FolderEntity byPath(String path){
        return (FolderEntity) FolderEntity.find("path",path).firstResult();
    }

    @Transactional
    public List<FolderEntity> list(){
        return FolderEntity.listAll();
    }

    @Override
    @Transactional
    public Map<String,Integer> getFolderUploadCount(){
        Map<String,Integer> rtrn = new HashMap<>();

        NativeQuery query = (NativeQuery) em.createNativeQuery(
            """
            select f.name as name, count(v.id) as count
            from folder f join node_group g on f.group_id = g.id join node r on r.id = g.root_id left join value v on v.node_id = r.id 
            group by f.name 
            """
        );
        List<Object[]> found = query
                .unwrap(NativeQuery.class)
                .addScalar("name", String.class)
                .addScalar("count", Integer.class)
                .getResultList();
        for( Object[] obj : found ){
            rtrn.put((String) obj[0], (Integer) obj[1]);
        }
        return rtrn;
    }

    @Transactional
    public long update(FolderEntity folder){
        FolderEntity.persist(folder);
        return folder.id;
    }

    @Override
    @Transactional
    public long delete(String name){
        return FolderEntity.delete("name", name);
    }

    @Override
    @Transactional
    public Json structure(String name) {
        FolderEntity folder = em.createQuery(
                "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                FolderEntity.class
        ).setParameter("name", name).getSingleResult();

        Json fullStructure = Json.typeStructure(new Json(false));
        NodeEntity root = folder.group.root;
        List<ValueEntity> uploads = valueService.getValues(root);
        for(ValueEntity upload : uploads){
            Json json = Json.fromJsonNode(upload.data);
            fullStructure.add(json);
        }
        return fullStructure;
    }


    /**
     * Schedules recalculation of all Values in the FolderEntity.
     * At the moment that means replacing all calculated values (no equality checking).
     * @param folder
     */
    @Override
    @Transactional
    public void recalculate(String name){
        FolderEntity folder = em.createQuery(
                "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                FolderEntity.class
        ).setParameter("name", name).getSingleResult();
        NodeEntity root = folder.group.root;
        List<ValueEntity> rootValues = valueService.getValues(root);
        rootValues.forEach(ValueEntity::getPath); // this fixes the LazyException, por que?
        List<Work> newWorks = new ArrayList<>();
        for(ValueEntity rootValue: rootValues){
            for(NodeEntity source : List.copyOf(folder.group.sources)){
                newWorks.add(new Work(source,new ArrayList<>(source.sources),List.of(rootValue)));
            }
        }
        workService.create(newWorks);
    }

    @Override
    @Transactional
    public void upload(String name, String path, JsonNode data){
        FolderEntity folder = em.createQuery(
                "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                FolderEntity.class
        ).setParameter("name", name).getSingleResult();
        ValueEntity newValue = new ValueEntity(folder,folder.group.root,data);
        valueService.create(newValue);

        //do we only queue the top level and let new values queue the remaining?
        //that would match the re-calculation workflow
        //or do we rely on workQueue for de-duplication?
/*
        folder.group.getTopLevelNodes().forEach(source ->{
            Work newWork = new Work(source,new ArrayList<>(source.sources),List.of(newValue));
            workService.create(newWork);
            workQueue.addWork(newWork);
        });
*/
        //List.copyOf is a hack to get around ConcurrentModificationException that is likely due to using entity list and panache setSources
//        List<Work> toQueue = List.copyOf(folder.group.sources).stream().map(source -> {
//            Work newWork = new Work(source,new ArrayList<>(source.sources),List.of(newValue));
//            workService.create(newWork);
//            return newWork;
//        }).toList();
//        workQueue.addWorks(toQueue);

        workService.create(List.copyOf(folder.group.sources).stream().map(source -> new Work(source, new ArrayList<>(source.sources), List.of(newValue))).toList());
    }
}
