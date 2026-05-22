package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.hyperfoil.tools.yaup.json.Json;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import org.hibernate.query.NativeQuery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class FolderService implements FolderServiceInterface {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    /*        if(!value.isPersistent()){
            ValueEntity merged = em.merge(value);
            em.flush();
            value.id = merged.id;
            return merged;
        }else{
            value.persist();
        }
        return value;

     */

    @Transactional
    public long create(FolderEntity entity){
        if(!entity.isPersistent()){
            FolderEntity merged = em.merge(entity);
            em.flush();
            entity.id = merged.id;
            return entity.id;
        }else{
            entity.persist();
        }
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
        FolderEntity entity = FolderEntity.find("name", name).firstResult();
        return entity != null ? apiMapper.toFolder(entity) : null;
    }
    @Transactional
    public FolderEntity byPath(String path){
        return (FolderEntity) FolderEntity.find("path",path).firstResult();
    }

    @Transactional
    public List<Folder> list(){
        return FolderEntity.<FolderEntity>streamAll().map(apiMapper::toFolder).toList();
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

        workService.create(folder.group.getTopLevelNodes().stream().map(node-> new Work(node,new ArrayList<>(node.sources),List.of(newValue))).toList());
        //workService.create(List.copyOf(folder.group.sources).stream().map(source -> new Work(source, new ArrayList<>(source.sources), List.of(newValue))).toList());
    }

    /**
     * Exports a folder's node graph to a JSON file.
     *
     * @param folderName the folder to export
     * @param outputPath path to write the JSON file
     */
    @Transactional
    public void export(String folderName, Path outputPath) throws IOException {
        FolderEntity folder = em.createQuery(
            "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
            FolderEntity.class
        ).setParameter("name", folderName).getSingleResult();

        ObjectNode root = MAPPER.createObjectNode();
        root.put("folder", folderName);
        ArrayNode nodesArray = root.putArray("nodes");

        // Root node first, then remaining nodes ordered by id.
        // Auto-increment ids ensure parents are always created before children,
        // so ORDER BY id is a valid topological order.
        NodeEntity rootNode = folder.group.root;
        nodesArray.add(serializeNode(rootNode));

        em.createQuery("SELECT n FROM node n WHERE n.group.id = ?1 AND n.id != ?2 ORDER BY n.id", NodeEntity.class)
            .setParameter(1, folder.group.id)
            .setParameter(2, rootNode.id)
            .getResultList()
            .forEach(n -> nodesArray.add(serializeNode(n)));

        Files.writeString(outputPath, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root));
        Log.infof("Exported folder '%s' with %d nodes to %s", folderName, nodesArray.size(), outputPath);
    }

    /**
     * Imports a folder and its node graph from a JSON file previously created by {@link #export}.
     *
     * @param inputPath path to the JSON file
     * @param overwrite if true, delete existing folder before importing
     * @return the folder name that was imported
     */
    @Transactional
    public String importFolder(Path inputPath, boolean overwrite) throws IOException {
        JsonNode root = MAPPER.readTree(inputPath.toFile());
        String folderName = root.get("folder").asText();
        JsonNode nodeArray = root.get("nodes");

        FolderEntity existing = em.createQuery(
            "SELECT f FROM folder f WHERE f.name = :name", FolderEntity.class
        ).setParameter("name", folderName).getResultStream().findFirst().orElse(null);

        if (existing != null) {
            if (!overwrite) {
                Log.infof("Folder '%s' already exists, skipping (use --overwrite to replace)", folderName);
                return folderName;
            }
            delete(folderName);
        }

        long folderId = create(folderName);
        FolderEntity folder = read(folderId);
        NodeGroupEntity group = folder.group;

        Map<Long, NodeEntity> idMap = new HashMap<>();
        idMap.put(nodeArray.get(0).get("id").asLong(), group.root);

        for (int i = 1; i < nodeArray.size(); i++) {
            JsonNode n = nodeArray.get(i);
            long exportedId = n.get("id").asLong();
            String name = n.get("name").asText();
            String type = n.get("type").asText();
            String operation = n.has("operation") && !n.get("operation").isNull()
                ? n.get("operation").asText() : "";

            List<NodeEntity> sources = new ArrayList<>();
            if (n.has("sources") && !n.get("sources").isNull()) {
                for (JsonNode srcId : n.get("sources")) {
                    NodeEntity src = idMap.get(srcId.asLong());
                    if (src != null) {
                        sources.add(src);
                    } else {
                        Log.warnf("Could not resolve source id %d for node '%s'", srcId.asLong(), name);
                    }
                }
            }

            NodeEntity node = switch (type) {
                case "jq" -> new JqNode(name, operation, sources);
                case "ecma" -> new JsNode(name, operation, sources);
                case "sql" -> new SqlJsonpathNode(name, operation, sources);
                case "sqlall" -> new SqlJsonpathAllNode(name, operation, sources);
                case "split" -> new SplitNode(name, operation, sources);
                case "fp" -> {
                    FingerprintNode fp = new FingerprintNode(name, operation);
                    fp.sources = new ArrayList<>(sources);
                    yield fp;
                }
                case "ft" -> {
                    FixedThreshold ft = new FixedThreshold(name, operation);
                    ft.sources = new ArrayList<>(sources);
                    yield ft;
                }
                case "rd" -> {
                    RelativeDifference rd = new RelativeDifference(name, operation);
                    rd.sources = new ArrayList<>(sources);
                    yield rd;
                }
                default -> {
                    Log.warnf("Unknown node type '%s' for node '%s', treating as jq", type, name);
                    yield new JqNode(name, operation, sources);
                }
            };

            node.group = group;
            NodeEntity merged = em.merge(node);
            group.sources.add(merged);
            idMap.put(exportedId, merged);
        }

        em.flush();
        em.merge(group);
        Log.infof("Imported folder '%s' with %d nodes from %s", folderName, nodeArray.size(), inputPath);
        return folderName;
    }

    private ObjectNode serializeNode(NodeEntity node) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("id", node.id);
        n.put("name", node.name != null ? node.name : "");
        n.put("type", discriminatorValue(node));
        if (node.operation != null && !node.operation.isBlank()) {
            n.put("operation", node.operation);
        } else {
            n.putNull("operation");
        }
        ArrayNode sources = n.putArray("sources");
        if (node.sources != null) {
            for (NodeEntity source : node.sources) {
                sources.add(source.id);
            }
        }
        return n;
    }

    /** Returns the JPA discriminator value for a node (e.g. "jq", "ecma", "sqlall"). */
    private String discriminatorValue(NodeEntity node) {
        var ann = node.getClass().getAnnotation(jakarta.persistence.DiscriminatorValue.class);
        return ann != null ? ann.value() : node.type().display();
    }
}
