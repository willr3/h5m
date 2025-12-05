package exp.svc;

import exp.entity.Folder;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.queue.WorkQueue;
import exp.queue.WorkQueueExecutor;
import io.hyperfoil.tools.yaup.json.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.io.File;
import java.util.List;

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
    public void scan(Folder folder){
        folder = Folder.findById(folder.id); // deal with detached entity
        File folderPath = new File(folder.path);
        List<File> todo = List.of(folderPath.listFiles(s -> s.toString().endsWith(".json") && !s.getName().startsWith(".")));

        List<Node> sources = folder.group.sources;
        for (File t : todo){
            String sourcePath = t.getPath();
            Value existing = valueService.byPath(sourcePath);
            if(existing == null){
                Value newValue = new Value(folder, folder.group.root, sourcePath);
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
