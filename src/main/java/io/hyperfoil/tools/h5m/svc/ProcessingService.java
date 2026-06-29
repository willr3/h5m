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
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;

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
        List<ProcessingTrackerEntity> incomplete = ProcessingTrackerEntity.find("completed", false).list();
        if (!incomplete.isEmpty()) {
            Log.infof("Found %d incomplete processing operations to recover", incomplete.size());
            for (ProcessingTrackerEntity tracking : incomplete) {
                switch (tracking.type) {
                    case UPLOAD -> recoverUpload(tracking);
                    case RECALCULATE -> recoverRecalculate(tracking);
                    case RECALCULATE_NODE -> recoverRecalculateNode(tracking);
                }
            }
        }
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
                .map(node -> new Work(node, new ArrayList<>(node.sources), List.of(rootValue)))
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

    private void recoverRecalculate(ProcessingTrackerEntity tracking) {
        FolderEntity folder = findFolderById(tracking.folderId);
        if (folder == null) {
            Log.warnf("Folder %d not found for incomplete recalculation, removing tracking record", tracking.folderId);
            tracking.delete();
            return;
        }
        Log.infof("Re-triggering full recalculation for folder '%s' (id=%d)", folder.name, tracking.folderId);
        tracking.completed = true;
        recalculate(folder.name);
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
                ProcessingType.RECALCULATE, tracking.folderId, -1);
        recoveryTracker.persist();
        tracking.completed = true;

        List<ValueEntity> rootValues = valueService.getValues(folder.group.root);
        rootValues.forEach(ValueEntity::getPath);
        List<Work> works = new ArrayList<>();
        Set<Long> rootValueIds = new HashSet<>();
        for (ValueEntity rootValue : rootValues) {
            rootValueIds.add(rootValue.id);
            for (NodeEntity sourceNode : List.copyOf(folder.group.sources)) {
                Work w = new Work(sourceNode, new ArrayList<>(sourceNode.sources), List.of(rootValue));
                w.dispatch = false;
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
     * Recalculates all values in the folder by reprocessing each root value
     * through the node graph. Queues top-level nodes for each root value and
     * relies on cascade in {@code WorkService.execute()} for downstream nodes.
     *
     * @param name the folder name to recalculate
     * @return recalculation status with progress tracking
     * @throws IllegalStateException if uploads are in progress for this folder
     */
    public RecalculationTracker recalculate(String name) {
        return QuarkusTransaction.requiringNew().call(() -> {
            FolderEntity folder = findFolderByName(name);
            List<NodeEntity> topLevelNodes = folder.group.getTopLevelNodes();
            return doRecalculate(folder, -1, ProcessingType.RECALCULATE, topLevelNodes);
        });
    }

    /**
     * Selectively recalculates values for a specific node and its dependents.
     * Walks up the source chain to find the highest ancestor nodes whose
     * source data is available (not ephemeral-nullified). Queues Work from
     * those ancestors — cascade in WorkService.execute() recomputes the
     * intermediate nodes before reaching the target.
     *
     * Use case: user changes a JQ expression or detection config for a node.
     *
     * @param nodeId the node to recalculate
     * @return recalculation status with progress tracking
     * @throws IllegalStateException if uploads are in progress for this folder
     * @throws IllegalArgumentException if the node is not found or has no group
     */
    public RecalculationTracker recalculateNode(long nodeId) {
        return QuarkusTransaction.requiringNew().call(() -> {
            NodeEntity targetNode = NodeEntity.findById(nodeId);
            if (targetNode == null) {
                throw new IllegalArgumentException("Node not found: " + nodeId);
            }
            if (targetNode.group == null) {
                throw new IllegalArgumentException("Node " + nodeId + " has no group");
            }
            FolderEntity folder = findFolderByGroupId(targetNode.group.id);
            Set<NodeEntity> startNodes = findRecomputationStartNodes(targetNode, folder.group.root);
            return doRecalculate(folder, nodeId, ProcessingType.RECALCULATE_NODE, startNodes);
        });
    }

    /**
     * Shared recalculation logic for both full-folder and selective-node recalculation.
     * Rejects if uploads are in progress, builds Work items for each root value × node,
     * creates a crash-recovery tracker, wires progress callbacks, and schedules
     * ephemeral data nullification on completion.
     *
     * @param folder the folder to recalculate (must be fetched with group, sources, root)
     * @param nodeId the target node ID, or -1 for full folder recalculation
     * @param trackingType RECALCULATE or RECALCULATE_NODE
     * @param nodesToQueue the nodes to create Work items for
     * @return recalculation status with progress tracking
     */
    private RecalculationTracker doRecalculate(FolderEntity folder, long nodeId,
                                               ProcessingType trackingType,
                                               Collection<NodeEntity> nodesToQueue) {
        // Reject if uploads are in progress to avoid conflicting Work items
        // with different dispatch flags (upload=true vs recalculate=false)
        long inFlight = ProcessingTrackerEntity.count(
                "type = ?1 and folderId = ?2 and completed = false", ProcessingType.UPLOAD, folder.id);
        if (inFlight > 0) {
            throw new IllegalStateException(
                    "Cannot recalculate while " + inFlight + " upload(s) are in progress for folder " + folder.name);
        }

        List<ValueEntity> rootValues = valueService.getValues(folder.group.root);
        rootValues.forEach(ValueEntity::getPath); // initialize lazy proxies

        if (rootValues.isEmpty()) {
            return recalculationService.create(folder.name, nodeId, 0, COMPLETED);
        }

        List<Work> works = new ArrayList<>();
        Set<Long> rootValueIds = new HashSet<>();
        for (ValueEntity rootValue : rootValues) {
            rootValueIds.add(rootValue.id);
            for (NodeEntity node : nodesToQueue) {
                Work w = new Work(node, new ArrayList<>(node.sources), List.of(rootValue));
                w.dispatch = false; // suppress external notifications during recalculation
                works.add(w);
            }
        }

        if (works.isEmpty()) {
            return recalculationService.create(folder.name, nodeId, 0, COMPLETED);
        }

        // Track for crash recovery
        ProcessingTrackerEntity tracking = new ProcessingTrackerEntity(trackingType, folder.id, nodeId);
        tracking.persist();

        CompletableFuture<Void> future = workService.createTracked(works, rootValueIds);

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
    }

    // --- Ephemeral chain walking ---

    /**
     * Walks up the source chain from the target node to find the nodes where
     * recomputation should start. Ephemeral nodes (DISCARD or AUTO with
     * non-detection children) will have their data nullified, so the pipeline
     * must be restarted from an ancestor whose data is available.
     *
     * The algorithm:
     * - If the target node's sources all have available data (KEEP, or root),
     *   return just the target node.
     * - If any source is ephemeral, recursively walk up to that source's sources.
     * - Stop at root nodes (always have data) or KEEP nodes.
     * - Return the set of nodes that should be queued as Work items.
     *   The cascade mechanism handles recomputing everything between the
     *   start nodes and the target.
     */
    Set<NodeEntity> findRecomputationStartNodes(NodeEntity targetNode, NodeEntity rootNode) {
        Set<NodeEntity> startNodes = new LinkedHashSet<>();
        findStartNodes(targetNode, rootNode, startNodes, new HashSet<>());
        return startNodes;
    }

    private void findStartNodes(NodeEntity node, NodeEntity rootNode,
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
                if (isEphemeral(source)) {
                    // This source's data is nullified — walk up further
                    allSourcesHaveData = false;
                    findStartNodes(source, rootNode, startNodes, visited);
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
     * Only DISCARD nodes are considered ephemeral here — matching the behavior
     * of nullifyEphemeralData() which only nullifies DISCARD nodes.
     * AUTO nodes are NOT treated as ephemeral on this branch because
     * markAutoEphemeral() (which resolves AUTO → DISCARD) is not called
     * during upload/recalculate. AUTO handling is addressed in PR #143.
     */
    private boolean isEphemeral(NodeEntity node) {
        return node.ephemeral == EphemeralMode.DISCARD;
    }
}
