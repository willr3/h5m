package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.api.ProcessingType;

import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ProcessingTrackerEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.quarkus.logging.Log;
import io.quarkus.narayana.jta.QuarkusTransaction;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.ConfigUtils;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Handles pipeline processing lifecycle: recalculation, selective node
 * recalculation, and crash recovery. Owns all {@link ProcessingTrackerEntity}
 * management for recalculation and recovery operations.
 *
 * <p>Upload processing remains in {@link FolderService} because it is tightly
 * coupled to root value creation. The recalculation methods here are exposed
 * via {@link FolderService} delegation to keep the {@code FolderServiceInterface}
 * contract stable.</p>
 */
@ApplicationScoped
public class ProcessingService {

    private static final String FOLDER_FETCH =
            "SELECT f FROM folder f JOIN FETCH f.group g LEFT JOIN FETCH g.sources LEFT JOIN FETCH g.root";

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    @Inject
    EntityManager em;
    @Inject
    ValueService valueService;
    @Inject
    WorkService workService;
    @Inject
    RecalculationService recalculationService;
    @Inject
    NodeService nodeService;

    // --- Recovery ---

    /**
     * On startup, re-trigger processing for any uploads or recalculations that
     * were interrupted (e.g., by a crash). Uses all source nodes (not just
     * top-level) so that mid-cascade crashes are recovered correctly — the
     * deduplication logic in execute() skips already-computed values while
     * ensuring missing children are still calculated.
     */
    @Transactional
    public void recoverIncompleteProcessing(@Observes @Priority(2) StartupEvent ev) {
        //ev == null when forced to recover
        if(!ConfigUtils.getProfiles().contains("cli") || ev == null) {
            List<ProcessingTrackerEntity> incomplete = ProcessingTrackerEntity.find("completed", false).list();
            if (!incomplete.isEmpty()) {
                Log.infof("Found %d incomplete processing operations to recover", incomplete.size());
                for (ProcessingTrackerEntity tracking : incomplete) {
                    switch (tracking.type) {
                        case UPLOAD -> recoverUpload(tracking);
                        case RECALCULATE_NODE -> recoverRecalculateNode(tracking);
                    }
                }
            }
        }
    }

    @Transactional
    public List<ProcessingTrackerEntity> getIncompleteProcessing() {
        return ProcessingTrackerEntity.find("completed", false).list();
    }

    @Transactional
    public int removeIncompleteProcessing(){
        List<ProcessingTrackerEntity> incomplete = getIncompleteProcessing();
        incomplete.forEach(ProcessingTrackerEntity::delete);
        return incomplete.size();
    }

    private void recoverUpload(ProcessingTrackerEntity tracking) {
        ValueEntity rootValue = ValueEntity.findById(tracking.referenceId);
        if (rootValue == null) {
            Log.warnf("Root value %d not found for incomplete upload, removing tracking record", tracking.referenceId);
            tracking.delete();
            return;
        }
        FolderEntity folder = findFolderById(tracking.folderId);
        if (folder == null) {
            Log.warnf("Folder %d not found for incomplete upload, removing tracking record", tracking.folderId);
            tracking.delete();
            return;
        }
        Log.infof("Re-triggering processing for upload %d in folder %d", tracking.referenceId, tracking.folderId);
        // Use all source nodes (not just top-level) to handle mid-cascade crashes
        List<Work> works = List.copyOf(folder.group.sources).stream()
                .map(node -> {
                    Work w = new Work(node, new ArrayList<>(node.sources), List.of(rootValue.id));
                    w.setCascade(false);
                    return w;
                })
                .toList();
        if (!works.isEmpty()) {
            CompletableFuture<Void> future = workService.createTracked(works, Set.of(rootValue.id));
            future.whenComplete((v, t) -> {
                QuarkusTransaction.requiringNew().run(() -> {
                    ProcessingTrackerEntity entity = ProcessingTrackerEntity.find(
                            "type = ?1 and referenceId = ?2", ProcessingType.UPLOAD, rootValue.id).firstResult();
                    if (entity != null) {
                        entity.completed = true;
                    }
                    valueService.nullifyEphemeralData(rootValue.id);
                    // Evict cached ValueEntity instances since data was nullified via native SQL
                    em.getEntityManagerFactory().getCache().evict(ValueEntity.class);
                });
            });
        } else {
            tracking.completed = true;
        }
    }

    private void recoverRecalculateNode(ProcessingTrackerEntity tracking) {
        FolderEntity folder = findFolderById(tracking.folderId);
        if (folder == null) {
            Log.warnf("Folder %d not found for incomplete node recalculation, removing tracking record", tracking.folderId);
            tracking.delete();
            return;
        }
        NodeEntity node = NodeEntity.findById(tracking.referenceId);
        if (node == null) {
            Log.warnf("Node %d not found for incomplete recalculation, removing tracking record", tracking.referenceId);
            tracking.delete();
            return;
        }
        // Recovery queues ALL source nodes (not just top-level or the specific node).
        //
        // Why not use recalculate() (top-level + cascade):
        // A mid-process crash may have left the pipeline in a partially computed
        // state — e.g., node A was recomputed but cascade to B, C didn't happen.
        // With top-level + cascade, A's dedup sees the value is already correct
        // and skips it without cascading. B and C remain stale.
        //
        // Why not use recalculateNode():
        // Same dedup issue — if the tracked node's value already matches, cascade
        // doesn't fire for its dependents.
        //
        // Solution: queue Work for EVERY node in the graph (same as upload recovery).
        // Each node gets its own Work item. The dedup logic skips nodes whose values
        // are already correct but processes any that need updating. This ensures the
        // entire pipeline reaches a consistent state regardless of where the crash
        // interrupted processing.
        Log.infof("Re-triggering processing for all nodes in folder %d (node %d was in progress)", tracking.folderId, tracking.referenceId);

        // Create a new tracker for this recovery work — if recovery itself crashes,
        // the new tracker ensures it's re-triggered on next startup.
        ProcessingTrackerEntity recoveryTracker = new ProcessingTrackerEntity(
                ProcessingType.RECALCULATE_NODE, tracking.folderId, tracking.referenceId);
        recoveryTracker.persist();
        tracking.completed = true;

        List<ValueEntity> rootValues = valueService.getValues(folder.group.root);
        rootValues.forEach(ValueEntity::getPath);
        List<Work> works = new ArrayList<>();
        Set<Long> rootValueIds = new HashSet<>();
        for (ValueEntity rootValue : rootValues) {
            rootValueIds.add(rootValue.id);
            for (NodeEntity sourceNode : List.copyOf(folder.group.sources)) {
                Work w = new Work(sourceNode, new ArrayList<>(sourceNode.sources), List.of(rootValue.id));
                w.setDispatch(false);
                w.setCascade(false);
                works.add(w);
            }
        }
        if (!works.isEmpty()) {
            long recoveryTrackerId = recoveryTracker.id;
            CompletableFuture<Void> future = workService.createTracked(works, rootValueIds);
            future.whenComplete((v, t) -> {
                QuarkusTransaction.requiringNew().run(() -> {
                    ProcessingTrackerEntity entity = ProcessingTrackerEntity.findById(recoveryTrackerId);
                    if (entity != null) {
                        entity.completed = true;
                    }
                    for (ValueEntity rootValue : rootValues) {
                        valueService.nullifyEphemeralData(rootValue.id);
                    }
                    // Evict cached ValueEntity instances since data was nullified via native SQL
                    em.getEntityManagerFactory().getCache().evict(ValueEntity.class);
                });
            });
        } else {
            recoveryTracker.completed = true;
        }
    }

    // --- Folder lookup helpers ---

    private FolderEntity findFolderById(long folderId) {
        return em.createQuery(FOLDER_FETCH + " WHERE f.id = :id", FolderEntity.class)
                .setParameter("id", folderId).getResultStream().findFirst().orElse(null);
    }

    private FolderEntity findFolderByName(String name) {
        return em.createQuery(FOLDER_FETCH + " WHERE f.name = :name", FolderEntity.class)
                .setParameter("name", name).getResultStream().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Folder not found: " + name));
    }

    private FolderEntity findFolderByGroupId(long groupId) {
        return em.createQuery(FOLDER_FETCH + " WHERE f.group.id = :groupId", FolderEntity.class)
                .setParameter("groupId", groupId).getResultStream().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No folder found for group " + groupId));
    }

    // --- Recalculation ---

    /**
     * Selectively recalculates values for a specific node and its dependents.
     * Walks up the source chain to find ephemeral ancestor.
     * Queues Work from those ancestors along with Work for target node
     *
     * Use cases:
     * * A user adds a new Node to the NodeGroup
     * * A user changes a Node's operation or sources
     *
     * @param nodeId the node to recalculate
     * @return recalculation status with progress tracking
     * @throws IllegalStateException if uploads are in progress for this folder
     * @throws IllegalArgumentException if the node is not found or has no group
     */
    public RecalculationTracker recalculateNode(long nodeId){
        return QuarkusTransaction.requiringNew().call(() -> {
            NodeEntity targetNode = NodeEntity.findById(nodeId);
            if(targetNode == null){
                throw new IllegalArgumentException("Node not found: " + nodeId);
            }
            if (targetNode.group == null) {
                throw new IllegalArgumentException("Node " + nodeId + " has no group");
            }
            FolderEntity folder = findFolderByGroupId(targetNode.group.id);
            List<ValueEntity> rootValues = valueService.getValues(targetNode.group.root);
            if (rootValues.isEmpty()) {
                return recalculationService.create(folder.name, nodeId, 0, COMPLETED);
            }
            rootValues.forEach(ValueEntity::getPath); // initialize lazy proxies

            Set<Long> rootValueIds = new HashSet<>(rootValues.size());
            Set<NodeEntity> ephemeralSources = nodeService.getEphemeralSources(targetNode);
            List<Work> todo = new  ArrayList<>();

            for(ValueEntity rootValue : rootValues){
                rootValueIds.add(rootValue.id);
                if(!ephemeralSources.isEmpty()){
                    for(NodeEntity ephemeralSource : ephemeralSources){
                        Work ephemeralWork = new Work(ephemeralSource,new ArrayList<>(ephemeralSource.sources),List.of(rootValue.id));
                        ephemeralWork.setCascade(false);
                        ephemeralWork.setDispatch(false);
                        todo.add(ephemeralWork);
                    }
                }
                Work nodeWork = new Work(targetNode, new ArrayList<>(targetNode.sources), List.of(rootValue.id));
                nodeWork.setDispatch(false);
                todo.add(nodeWork);
            }

            if(todo.isEmpty()){
                return recalculationService.create(folder.name, nodeId, 0, COMPLETED);
            }

            // Track for crash recovery
            ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(ProcessingType.RECALCULATE_NODE, folder.id, nodeId);
            tracking.persist();

            CompletableFuture<Void> future = workService.createTracked(todo, rootValueIds);

            // Create status tracker with per-root progress callbacks.
            // This wiring is safe from races: createTracked() defers work queue insertion
            // to afterCompletion (transaction commit), so no work has started yet.
            RecalculationTracker status = recalculationService.create(folder.name, nodeId, rootValues.size(), future);
            for (Long rootId : rootValueIds) {
                workService.getTracker(rootId).ifPresent(tracker ->
                        tracker.getFuture().whenComplete((v, t) -> status.incrementCompleted())
                );
            }

            // Mark completed and null out ephemeral data after recalculation finishes
            long trackingId = tracking.id;
            future.whenComplete((v, t) -> {
                if (t != null) {
                    Log.errorf(t, "Recalculation failed for folder '%s' (nodeId=%d)", folder.name, nodeId);
                }
                // Mark tracker completed even on failure to prevent infinite retry on restart.
                // On success, also nullify ephemeral data and evict the 2LC.
                QuarkusTransaction.requiringNew().run(() -> {
                    ProcessingTrackerEntity entity = ProcessingTrackerEntity.findById(trackingId);
                    if (entity != null) {
                        entity.completed = true;
                    }
                    if (t == null) {
                        for (ValueEntity rootValue : rootValues) {
                            int nullified = valueService.nullifyEphemeralData(rootValue.id);
                            if (nullified > 0) {
                                Log.debugf("Nullified data for %d ephemeral values (root %d)", nullified, rootValue.id);
                            }
                        }
                        // Evict cached ValueEntity instances since data was nullified via native SQL
                        em.getEntityManagerFactory().getCache().evict(ValueEntity.class);
                    }
                });
            });
            return status;

        });
    }

    // --- Ephemeral chain walking ---
    /**
     * Walks up the source chain from the target node to find the nodes where
     * recomputation should start. Ephemeral nodes (DISCARD, or AUTO with
     * non-detection children) will have their data nullified, so the pipeline
     * must be restarted from an ancestor whose data is available.
     *
     * <p>The algorithm:</p>
     * <ul>
     *   <li>If the target node's sources all have available data (KEEP, root,
     *       or detection sources), return just the target node.</li>
     *   <li>If any source is ephemeral (data nullified), recursively walk up
     *       to that source's sources.</li>
     *   <li>Stop at root nodes (always have data) or KEEP nodes.</li>
     *   <li>Return the set of nodes that should be queued as Work items.
     *       The cascade mechanism handles recomputing everything between the
     *       start nodes and the target.</li>
     * </ul>
     *
     * @param allGroupNodes all nodes in the folder's group (already loaded via
     *                      JOIN FETCH), used to check graph structure in-memory
     */
    Set<NodeEntity> findRecomputationStartNodes(NodeEntity targetNode, NodeEntity rootNode,
                                                  List<NodeEntity> allGroupNodes) {
        Set<NodeEntity> startNodes = new LinkedHashSet<>();
        findStartNodes(targetNode, rootNode, allGroupNodes, startNodes, new HashSet<>());
        return startNodes;
    }

    private void findStartNodes(NodeEntity node, NodeEntity rootNode,
                                 List<NodeEntity> allGroupNodes,
                                 Set<NodeEntity> startNodes, Set<Long> visited) {
        if (!visited.add(node.getId())) {
            return; // avoid cycles
        }

        boolean allSourcesHaveData = true;
        if (node.sources != null) {
            for (NodeEntity source : node.sources) {
                if (source.getId().equals(rootNode.getId())) {
                    // Root always has data — this source is fine
                    continue;
                }
                if (isEphemeral(source, allGroupNodes)) {
                    // This source's data is nullified — walk up further
                    allSourcesHaveData = false;
                    findStartNodes(source, rootNode, allGroupNodes, startNodes, visited);
                }
                // KEEP or detection sources have data — no need to walk up
            }
        }

        if (allSourcesHaveData) {
            // All sources have data available — this node can be the start point
            startNodes.add(node);
        }
        // If not all sources have data, the recursive calls above will add
        // the correct ancestors to startNodes. This node will be reached
        // via cascade from those ancestors.
    }

    /**
     * Checks if a node's value data has been ephemeral-nullified.
     * Matches the nullifyEphemeralData() SQL logic:
     * <ul>
     *   <li>KEEP — never ephemeral</li>
     *   <li>DISCARD — always ephemeral</li>
     *   <li>AUTO — ephemeral only if the node has non-detection children
     *       and is not itself a direct source of a detection node</li>
     * </ul>
     * Uses the already-loaded group nodes to check graph structure in-memory.
     */
    private boolean isEphemeral(NodeEntity node, List<NodeEntity> allGroupNodes) {
        if (node.ephemeral == EphemeralMode.KEEP) return false;
        if (node.ephemeral == EphemeralMode.DISCARD) return true;
        // AUTO: check graph structure to determine if data was nullified
        boolean hasNonDetectionChild = false;
        boolean isDetectionSource = false;
        for (NodeEntity other : allGroupNodes) {
            if (other.sources != null && other.sources.stream()
                    .anyMatch(s -> s.getId().equals(node.getId()))) {
                if (other.type().isDetection()) {
                    isDetectionSource = true;
                } else {
                    hasNonDetectionChild = true;
                }
            }
        }
        return hasNonDetectionChild && !isDetectionSource;
    }

    public void deleteForFolder(long folderId) {
        em.createNativeQuery("DELETE FROM processing_tracker WHERE folder_id = :fid")
                .setParameter("fid", folderId).executeUpdate();
    }
}
