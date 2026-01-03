package exp.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import exp.entity.Folder;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.queue.WorkQueue;
import exp.queue.WorkQueueExecutor;
import exp.queue.WorkRunner;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.query.NativeQuery;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class FolderService {

    @Inject
    EntityManager em;
    @Inject
    ValueService valueService;

    @Inject
    @Named("workExecutor")
    WorkQueueExecutor workExecutor;

    @Inject
    WorkService workService;

    @Transactional
    public long create(Folder folder){
        if(!folder.isPersistent()){
            Folder.persist(folder);
        }
        return folder.id;
    }

    @Transactional
    public Folder read(long id){
        return Folder.findById(id);
    }

    @Transactional
    public Folder byName(String name){
        return (Folder) Folder.find("name",name).firstResult();
    }
    @Transactional
    public Folder byPath(String path){
        return (Folder) Folder.find("path",path).firstResult();
    }

    @Transactional
    public List<Folder> list(){
        return Folder.listAll();
    }

    @Transactional
    public Map<String,Integer> getFolderUploadCount(){
        Map<String,Integer> rtrn = new HashMap<>();

        NativeQuery query = (NativeQuery) em.createNativeQuery(
            """
            select f.name as name, count(v.id) as count
            from folder f join nodegroup g on f.group_id = g.id join node r on r.id = g.root_id left join value v on v.node_id = r.id 
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
    public long update(Folder folder){
        Folder.persist(folder);
        return folder.id;
    }

    @Transactional
    public void delete(Folder folder){
        folder.delete();
    }


    @Transactional
    public Json structure(Folder folder) {
        Json fullStructure = Json.typeStructure(new Json(false));
        folder = Folder.findById(folder.id); // deal with detached entity
        Node root = folder.group.root;
        List<Value> uploads = valueService.getValues(root);
        for(Value upload : uploads){
            Json json = Json.fromJsonNode(upload.data);
            fullStructure.add(json);
        }
        return fullStructure;
    }


    /**
     * Schedules recalculation of all Values in the Folder.
     * At the moment that means replacing all calculated values (no equality checking).
     * @param folder
     */
    @Transactional
    public void recalculate(Folder folder){
        folder = Folder.findById(folder.id); // deal with detached entity
        Node root = folder.group.root;
        List<Value> rootValues = valueService.getValues(root);
        rootValues.forEach(Value::getPath); // this fixes the LazyException, por que?
        List<Work> newWorks = new ArrayList<>();
        for(Value rootValue: rootValues){
            for(Node source : List.copyOf(folder.group.sources)){
                Work newWork = new Work(source,new ArrayList<>(source.sources),List.of(rootValue));
                workService.create(newWork);
                newWorks.add(newWork);
            }
        }
        workExecutor.getWorkQueue().addWorks(newWorks);
    }
    @Transactional
    public void upload(Folder folder,String path,JsonNode data){
        folder = Folder.findById(folder.id); // deal with detached entity
        folder.group.sources.size();//deal with lazy init
        Value newValue = new Value(folder,folder.group.root,data);
        valueService.create(newValue);
        WorkQueue workQueue = workExecutor.getWorkQueue();

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
        List<Work> toQueue = List.copyOf(folder.group.sources).stream().map(source -> {
            Work newWork = new Work(source,new ArrayList<>(source.sources),List.of(newValue));
            workService.create(newWork);
            return newWork;
        }).toList();
        workQueue.addWorks(toQueue);
    }
}
