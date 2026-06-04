package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;
import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.UploadProcessingEntity;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.ViewEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import io.quarkus.narayana.jta.QuarkusTransaction;
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
import java.util.concurrent.CompletableFuture;
import java.time.LocalDateTime;
import java.util.*;

@ApplicationScoped
public class FolderService implements FolderServiceInterface {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static LocalDateTime toLocalDateTime(Object value) {
        if (value == null) return null;
        if (value instanceof java.sql.Timestamp ts) return ts.toLocalDateTime();
        if (value instanceof LocalDateTime ldt) return ldt;
        if (value instanceof java.time.OffsetDateTime odt) return odt.toLocalDateTime();
        if (value instanceof Number n) return LocalDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(n.longValue()), java.time.ZoneId.systemDefault());
        return null;
    }

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

    /**
     * On startup, re-trigger processing for any uploads that were interrupted
     * (e.g., by a crash). Uses all source nodes (not just top-level) so that
     * mid-cascade crashes are recovered correctly — the deduplication logic in
     * execute() skips already-computed values while ensuring missing children
     * are still calculated.
     */
    @Transactional
    public void recoverIncompleteUploads(@Observes @Priority(2) StartupEvent ev) {
        List<UploadProcessingEntity> incomplete = UploadProcessingEntity.find("completed", false).list();
        if (!incomplete.isEmpty()) {
            Log.infof("Found %d incomplete uploads to recover", incomplete.size());
            for (UploadProcessingEntity tracking : incomplete) {
                ValueEntity rootValue = ValueEntity.findById(tracking.rootValueId);
                if (rootValue == null) {
                    Log.warnf("Root value %d not found for incomplete upload, removing tracking record", tracking.rootValueId);
                    tracking.delete();
                    continue;
                }
                FolderEntity folder = em.createQuery(
                        "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                        FolderEntity.class
                ).setParameter("name", tracking.folderName).getResultStream().findFirst().orElse(null);
                if (folder == null) {
                    Log.warnf("Folder '%s' not found for incomplete upload, removing tracking record", tracking.folderName);
                    tracking.delete();
                    continue;
                }
                Log.infof("Re-triggering processing for upload %d in folder '%s'", tracking.rootValueId, tracking.folderName);
                // Use all source nodes (not just top-level) to handle mid-cascade crashes
                List<Work> works = List.copyOf(folder.group.sources).stream()
                        .map(node -> new Work(node, new ArrayList<>(node.sources), List.of(rootValue)))
                        .toList();
                if (!works.isEmpty()) {
                    CompletableFuture<Void> future = workService.createTracked(works, Set.of(rootValue.id));
                    future.whenComplete((v, t) -> {
                        QuarkusTransaction.requiringNew().run(() -> {
                            UploadProcessingEntity entity = UploadProcessingEntity.find("rootValueId", rootValue.id).firstResult();
                            if (entity != null) {
                                entity.completed = true;
                            }
                        });
                    });
                } else {
                    tracking.completed = true;
                }
            }
        }
    }

    @Override
    @Transactional
    public long create(String name){
        FolderEntity entity = new FolderEntity();
        entity.name = name;
        entity.group = new NodeGroupEntity(name); //TODO do we auto-create a nodeGroup?
        FolderEntity.persist(entity);
        createDefaultView(entity);
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

    /**
     * Returns dashboard summaries for all folders, including upload count,
     * node count, change count, and timestamps of last upload and change.
     */
    @Override
    @Transactional
    @SuppressWarnings("unchecked")
    public List<FolderSummary> getDashboardSummaries() {
        List<Object[]> rows = em.createNativeQuery("""
            SELECT
                f.id,
                f.name,
                COUNT(DISTINCT rv.id) AS upload_count,
                COUNT(DISTINCT n.id) AS node_count,
                COUNT(DISTINCT dv.id) AS change_count,
                MAX(rv.created_at) AS last_upload,
                MAX(dv.created_at) AS last_change
            FROM folder f
            JOIN node_group g ON f.group_id = g.id
            JOIN node r ON r.id = g.root_id
            LEFT JOIN value rv ON rv.node_id = r.id
            LEFT JOIN node n ON n.group_id = g.id AND n.id != r.id
            LEFT JOIN node dn ON dn.group_id = g.id AND dn.type IN ('ft', 'rd')
            LEFT JOIN value dv ON dv.node_id = dn.id
            GROUP BY f.id, f.name
            ORDER BY f.name
            """).getResultList();

        List<FolderSummary> summaries = new ArrayList<>();
        for (Object[] row : rows) {
            summaries.add(new FolderSummary(
                ((Number) row[0]).longValue(),
                (String) row[1],
                ((Number) row[2]).intValue(),
                ((Number) row[3]).intValue(),
                ((Number) row[4]).intValue(),
                toLocalDateTime(row[5]),
                toLocalDateTime(row[6])
            ));
        }
        return summaries;
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
        // Delete views and components before folder (bulk delete doesn't cascade)
        em.createNativeQuery("DELETE FROM folder_view_component WHERE view_id IN (SELECT id FROM folder_view WHERE folder_id IN (SELECT id FROM folder WHERE name = :name))")
            .setParameter("name", name).executeUpdate();
        em.createNativeQuery("DELETE FROM folder_view WHERE folder_id IN (SELECT id FROM folder WHERE name = :name)")
            .setParameter("name", name).executeUpdate();
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

    /**
     * Uploads data and returns a future that completes when all processing finishes.
     * Uses QuarkusTransaction to control the transaction boundary explicitly —
     * the transaction commits when the lambda returns, before we return the future.
     * This avoids the deadlock that occurs with @Transactional + CompletableFuture
     * return types (Quarkus defers commit waiting for the future to complete).
     */
    @Override
    public CompletableFuture<Void> upload(String name, String path, JsonNode data){
        return QuarkusTransaction.requiringNew().call(() -> {
            FolderEntity folder = em.createQuery(
                    "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                    FolderEntity.class
            ).setParameter("name", name).getSingleResult();
            ValueEntity newValue = new ValueEntity(folder, folder.group.root, data);
            valueService.create(newValue);

            // Track this upload for crash recovery — marked completed when all work finishes
            UploadProcessingEntity tracking = new UploadProcessingEntity(newValue.id, name);
            tracking.persist();

            //only queue the top level and let new values queue the remaining
            //that matches the re-calculation workflow
            List<Work> works = folder.group.getTopLevelNodes().stream()
                    .map(node -> new Work(node, new ArrayList<>(node.sources), List.of(newValue)))
                    .toList();

            if (works.isEmpty()) {
                tracking.completed = true;
                return CompletableFuture.<Void>completedFuture(null);
            }

            CompletableFuture<Void> future = workService.createTracked(works, Set.of(newValue.id));
            // Mark completed when all work finishes
            future.whenComplete((v, t) -> {
                QuarkusTransaction.requiringNew().run(() -> {
                    UploadProcessingEntity entity = UploadProcessingEntity.find("rootValueId", newValue.id).firstResult();
                    if (entity != null) {
                        entity.completed = true;
                    }
                });
            });
            return future;
        });
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

    /**
     * Creates an empty "Default" view for the folder.
     * Users configure which nodes appear as columns via the REST API.
     */
    private void createDefaultView(FolderEntity folder) {
        ViewEntity view = new ViewEntity("Default", folder);
        view.persist();
        folder.views.add(view);
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
