package exp.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import exp.entity.Folder;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.pasted.JsonBinaryType;
import exp.queue.WorkQueue;
import exp.queue.WorkQueueExecutor;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.hibernate.query.NativeQuery;

import java.io.File;
import java.io.IOException;
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
    public int uploadCount(Folder folder){
        File f = new File(folder.path);
        return f.list((dir, name) -> name.startsWith("upload.")).length;
    }

    public Json structure(Folder folder){
        File f = new File(folder.path);
        List<File> todo = List.of(f.listFiles(s -> s.toPath().endsWith(".json") && !s.getName().startsWith(".")));
        Json fullStructure = Json.typeStructure(new Json(false));
        for(File t: todo){
            fullStructure.add(Json.fromFile(t.getPath()));

        }
        return fullStructure;
    }


    @Transactional
    public void recalculate(Folder folder){
        folder = Folder.findById(folder.id); // deal with detached entity
        Node root = folder.group.root;
        List<Value> rootValues = valueService.getValues(root);
        for(Value rootValue: rootValues){
            folder.group.sources.forEach(source -> {
                workExecutor.getWorkQueue().addWork(
                        new Work(source,source.sources,List.of(rootValue))
                );
            });
        }
    }

    @Transactional
    public void scan(Folder folder){
        folder = Folder.findById(folder.id); // deal with detached entity
        File folderPath = new File(folder.path);
        List<File> todo = List.of(folderPath.listFiles(s -> s.toString().endsWith(".json") && !s.getName().startsWith(".")));

        List<Node> sources = folder.group.sources;
        for (File t : todo){
            String sourcePath = t.getPath();
            Value existing = valueService.byPath(sourcePath);
            if(existing == null){
                ObjectMapper objectMapper = new ObjectMapper();
                Value newValue = new Value(folder, folder.group.root, sourcePath);
                try {
                    newValue.data = objectMapper.readTree(t);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
                valueService.create(newValue);
                WorkQueue workQueue = workExecutor.getWorkQueue();
                folder.group.sources.forEach(source -> {
                    workQueue.addWork(new Work(source,source.sources,List.of(newValue)));
                });
            }else{
                System.err.println("value "+sourcePath+" already exists");
            }
        }
    }

}
