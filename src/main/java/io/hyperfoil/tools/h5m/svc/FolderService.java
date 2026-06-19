package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.Upload;
import io.hyperfoil.tools.jjq.value.JqArray;
import io.hyperfoil.tools.jjq.value.JqNull;
import io.hyperfoil.tools.jjq.value.JqNumber;
import io.hyperfoil.tools.jjq.value.JqObject;
import io.hyperfoil.tools.jjq.value.JqString;
import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.api.FolderSummary;

import io.hyperfoil.tools.h5m.api.svc.FolderServiceInterface;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ProcessingTrackerEntity;
import io.hyperfoil.tools.h5m.api.ProcessingType;
import io.hyperfoil.tools.h5m.entity.Team;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.ViewEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PessimisticLockException;
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
    ProcessingService processingService;

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
        createDefaultView(entity);
        return entity.id;
    }

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
            LEFT JOIN node dn ON dn.group_id = g.id AND dn.type IN ('ft', 'rd', 'sd', 'ed')
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
    public JqValue structure(String name) {
        FolderEntity folder = em.createQuery(
                "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                FolderEntity.class
        ).setParameter("name", name).getSingleResult();

        NodeEntity root = folder.group.root;
        List<ValueEntity> uploads = valueService.getValues(root);
        JqValue merged = JqObject.EMPTY;
        for (ValueEntity upload : uploads) {
            if (upload.data != null) {
                JqValue schema = JqValues.typeStructure(upload.data);
                merged = JqValues.mergeTypeStructures(merged, schema);
            }
        }
        return merged;
    }


    @Override
    public RecalculationTracker recalculate(String name) {
        return processingService.recalculate(name);
    }

    @Override
    public RecalculationTracker recalculateNode(long nodeId) {
        return processingService.recalculateNode(nodeId);
    }

    private static final int UPLOAD_RETRY_LIMIT = 3;

    private static boolean isPessimisticLock(Throwable t) {
        for (; t != null; t = t.getCause()) {
            if (t instanceof PessimisticLockException) return true;
        }
        return false;
    }

    /**
     * Uploads data and returns an UploadHandle containing the upload ID and a future
     * that completes when all processing finishes.
     * Uses QuarkusTransaction to control the transaction boundary explicitly —
     * the transaction commits when the lambda returns, before we return the future.
     * This avoids the deadlock that occurs with @Transactional + CompletableFuture
     * return types (Quarkus defers commit waiting for the future to complete).
     */
    @Override
    public Upload upload(String name, String path, JqValue data) {
        // SQLite allows only one writer at a time. Under concurrent uploads the
        // transaction can fail with SQLITE_BUSY (surfaced as PessimisticLockException)
        // when another connection commits between our read and write, invalidating
        // the WAL snapshot. Retry with a fresh transaction to get a current snapshot.
        for (int attempt = 1; ; attempt++) {
            try {
                return doUpload(name, path, data);
            } catch (Throwable t) {
                if (attempt >= UPLOAD_RETRY_LIMIT || !isPessimisticLock(t)) {
                    throw t;
                }
                Log.debugf("Database busy during upload (attempt %d/%d), retrying", attempt, UPLOAD_RETRY_LIMIT);
            }
        }
    }

    private Upload doUpload(String name, String path, JqValue data) {
        return QuarkusTransaction.requiringNew().call(() -> {
            FolderEntity folder = em.createQuery(
                    "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root WHERE f.name = :name",
                    FolderEntity.class
            ).setParameter("name", name).getSingleResult();
            ValueEntity newValue = new ValueEntity(folder, folder.group.root, data);
            valueService.create(newValue);

            // Track this upload for crash recovery — marked completed when all work finishes
            ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.UPLOAD, folder.id, newValue.id);
            tracking.persist();

            //only queue the top level and let new values queue the remaining
            //that matches the re-calculation workflow
            List<Work> works = folder.group.getTopLevelNodes().stream()
                    .map(node -> new Work(node, new ArrayList<>(node.sources), List.of(newValue)))
                    .toList();

            if (works.isEmpty()) {
                tracking.completed = true;
                return new Upload(newValue.id, CompletableFuture.completedFuture(null));
            }

            CompletableFuture<Void> future = workService.createTracked(works, Set.of(newValue.id));
            // Mark completed and null out ephemeral data when all work finishes
            future.whenComplete((v, t) -> {
                QuarkusTransaction.requiringNew().run(() -> {
                    ProcessingTrackerEntity entity = ProcessingTrackerEntity.find(
                            "type = ?1 and referenceId = ?2", ProcessingType.UPLOAD, newValue.id).firstResult();
                    if (entity != null) {
                        if (t != null) {
                            Log.errorf(t, "Upload %d failed during processing", newValue.id);
                        } else {
                            entity.completed = true;
                        }
                    }
                    // Null out data for ephemeral nodes to reclaim storage
                    int nullified = valueService.nullifyEphemeralData(newValue.id);
                    if (nullified > 0) {
                        Log.debugf("Nullified data for %d ephemeral values (upload %d)", nullified, newValue.id);
                        // Evict cached ValueEntity instances since data was nullified via native SQL
                        em.getEntityManagerFactory().getCache().evict(ValueEntity.class);
                    }
                });
            });
            return new Upload(newValue.id, future);
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

        // Root node first, then remaining nodes ordered by id.
        // Auto-increment ids ensure parents are always created before children,
        // so ORDER BY id is a valid topological order.
        NodeEntity rootNode = folder.group.root;
        java.util.List<JqValue> nodeList = new java.util.ArrayList<>();
        nodeList.add(serializeNode(rootNode));

        em.createQuery("SELECT n FROM node n WHERE n.group.id = ?1 AND n.id != ?2 ORDER BY n.id", NodeEntity.class)
            .setParameter(1, folder.group.id)
            .setParameter(2, rootNode.id)
            .getResultList()
            .forEach(n -> nodeList.add(serializeNode(n)));

        JqObject root = JqObject.of("folder", JqString.of(folderName),
            "nodes", JqArray.of(nodeList.toArray(new JqValue[0])));

        Files.writeString(outputPath, JqValues.toPrettyJsonString(root));
        Log.infof("Exported folder '%s' with %d nodes to %s", folderName, nodeList.size(), outputPath);
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
        JqValue root = JqValues.parse(Files.readString(inputPath));
        if (!(root instanceof JqObject rootObj)) {
            throw new IllegalArgumentException("Import file must contain a JSON object, got: " + root.getClass().getSimpleName());
        }
        String folderName = rootObj.get("folder").asString("");
        if (folderName.isBlank()) {
            throw new IllegalArgumentException("Import file must contain a non-empty 'folder' field");
        }
        JqValue nodesValue = rootObj.get("nodes");
        if (!(nodesValue instanceof JqArray nodeArray)) {
            throw new IllegalArgumentException("Import file must contain a 'nodes' array, got: " + (nodesValue != null ? nodesValue.getClass().getSimpleName() : "null"));
        }

        FolderEntity existing = em.createQuery(
            "SELECT f FROM folder f WHERE f.name = :name", FolderEntity.class
        ).setParameter("name", folderName).getResultStream().findFirst().orElse(null);

        if (existing != null) {
            if (!overwrite) {
                Log.infof("Folder '%s' already exists, skipping (use --overwrite to replace)", folderName);
                return folderName;
            }
            delete(folderName);
            em.flush();
            em.clear();
        }

        long folderId = create(folderName);
        FolderEntity folder = read(folderId);
        NodeGroupEntity group = folder.group;

        Map<Long, NodeEntity> idMap = new HashMap<>();
        JqObject firstNode = (JqObject) nodeArray.get(0);
        idMap.put(firstNode.get("id").asLong(0), group.root);

        for (int i = 1; i < nodeArray.length(); i++) {
            JqObject n = (JqObject) nodeArray.get(i);
            long exportedId = n.get("id").asLong(0);
            String name = n.get("name").asString("");
            String type = n.get("type").asString("");
            String operation = n.has("operation") && !n.get("operation").isNull()
                ? n.get("operation").asString("") : "";

            List<NodeEntity> sources = new ArrayList<>();
            if (n.has("sources") && !n.get("sources").isNull()) {
                JqArray srcArray = (JqArray) n.get("sources");
                for (int j = 0; j < srcArray.length(); j++) {
                    long srcId = srcArray.get(j).asLong(0);
                    NodeEntity src = idMap.get(srcId);
                    if (src != null) {
                        sources.add(src);
                    } else {
                        Log.warnf("Could not resolve source id %d for node '%s'", srcId, name);
                    }
                }
            }

            NodeEntity node = switch (type) {
                case "jq" -> new JqNode(name, operation, sources);
                case "ecma" -> new JsNode(name, operation, sources);
                // Convert legacy sql/sqlall jsonpath to jq equivalents
                case "sql" -> new JqNode(name, NodeService.jsonpathToJq(operation), sources);
                case "sqlall" -> new JqNode(name, NodeService.jsonpathToJqArray(operation), sources);
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
                case "sd" -> {
                    StdDevAnomaly sd = new StdDevAnomaly(name, operation);
                    sd.sources = new ArrayList<>(sources);
                    yield sd;
                }
                case "ed" -> {
                    EDivisive ed = new EDivisive(name, operation);
                    ed.sources = new ArrayList<>(sources);
                    yield ed;
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

        Log.infof("Imported folder '%s' with %d nodes from %s", folderName, nodeArray.length(), inputPath);
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

    private JqObject serializeNode(NodeEntity node) {
        JqObject.Builder builder = JqObject.builder();
        builder.put("id", node.id);
        builder.put("name", node.name != null ? node.name : "");
        builder.put("type", discriminatorValue(node));
        if (node.operation != null && !node.operation.isBlank()) {
            builder.put("operation", node.operation);
        } else {
            builder.put("operation", JqNull.NULL);
        }
        if (node.sources != null && !node.sources.isEmpty()) {
            JqValue[] srcIds = node.sources.stream()
                .map(s -> (JqValue) JqNumber.of(s.id))
                .toArray(JqValue[]::new);
            builder.put("sources", JqArray.of(srcIds));
        } else {
            builder.put("sources", JqArray.of());
        }
        return builder.build();
    }

    /** Returns the JPA discriminator value for a node (e.g. "jq", "ecma", "sqlall"). */
    private String discriminatorValue(NodeEntity node) {
        var ann = node.getClass().getAnnotation(jakarta.persistence.DiscriminatorValue.class);
        return ann != null ? ann.value() : node.type().display();
    }
}
