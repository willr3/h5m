package io.hyperfoil.tools.h5m.benchmark;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.svc.NodeService;
import jakarta.transaction.TransactionManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Database-persisting graph builder for benchmarks.
 * Creates real JPA entities via NodeService with proper edge table population.
 * All entity references are reloaded by ID after transaction boundaries to avoid detached entity issues.
 */
public class DbGraphBuilder {

    private static final int BATCH_SIZE = 50;

    private final NodeService nodeService;
    private final TransactionManager tm;

    public DbGraphBuilder(NodeService nodeService, TransactionManager tm) {
        this.nodeService = nodeService;
        this.tm = tm;
    }

    /**
     * Creates N independent JqNodes in one group, each with root as its only source.
     *
     * @return the group ID
     */
    public long buildFlat(String groupName, int nodeCount) throws Exception {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity(groupName);
        group.persist();
        long groupId = group.id;
        long rootId = group.root.id;
        tm.commit();

        int created = 0;
        while (created < nodeCount) {
            tm.begin();
            NodeEntity root = NodeEntity.findById(rootId);
            NodeGroupEntity g = NodeGroupEntity.findById(groupId);
            int batchEnd = Math.min(created + BATCH_SIZE, nodeCount);
            for (int i = created; i < batchEnd; i++) {
                JqNode node = new JqNode("n_" + i, ".n_" + i, root);
                node.group = g;
                nodeService.create(node);
            }
            tm.commit();
            created = batchEnd;
        }

        return groupId;
    }

    /**
     * Creates a linear chain: root -> n0 -> n1 -> ... -> n(depth-1).
     * Each node depends on the previous one.
     *
     * @return the group ID
     */
    public long buildChain(String groupName, int depth) throws Exception {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity(groupName);
        group.persist();
        long groupId = group.id;
        long previousId = group.root.id;
        tm.commit();

        int created = 0;
        while (created < depth) {
            tm.begin();
            NodeGroupEntity g = NodeGroupEntity.findById(groupId);
            int batchEnd = Math.min(created + BATCH_SIZE, depth);
            for (int i = created; i < batchEnd; i++) {
                NodeEntity previous = NodeEntity.findById(previousId);
                JqNode node = new JqNode("n_" + i, ".n_" + i, previous);
                node.group = g;
                NodeEntity merged = nodeService.create(node);
                previousId = merged.id;
            }
            tm.commit();
            created = batchEnd;
        }

        return groupId;
    }

    /**
     * Creates a layered DAG (diamond/lattice topology):
     * root -> layer1(width nodes) -> layer2(width nodes) -> ... -> layerN(width nodes)
     * <p>
     * Each node in layer L depends on ALL nodes in layer L-1.
     * This produces quadratic edge growth: width^2 edges per layer transition.
     *
     * @return the group ID
     */
    public long buildDiamond(String groupName, int layers, int width) throws Exception {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity(groupName);
        group.persist();
        long groupId = group.id;
        long rootId = group.root.id;
        tm.commit();

        buildDiamondInGroup(groupId, rootId, layers, width);
        return groupId;
    }

    /**
     * Creates N independent JqNodes in an existing group, each with the group's root as its only source.
     * Uses identity jq expression (.) so nodes produce values when data is uploaded.
     */
    public void buildFlatInGroup(long groupId, long rootId, int nodeCount) throws Exception {
        int created = 0;
        while (created < nodeCount) {
            tm.begin();
            NodeEntity root = NodeEntity.findById(rootId);
            NodeGroupEntity g = NodeGroupEntity.findById(groupId);
            int batchEnd = Math.min(created + BATCH_SIZE, nodeCount);
            for (int i = created; i < batchEnd; i++) {
                JqNode node = new JqNode("n_" + i, ".", root);
                node.group = g;
                nodeService.create(node);
            }
            tm.commit();
            created = batchEnd;
        }
    }

    /**
     * Creates a linear chain in an existing group: root -> n0 -> n1 -> ... -> n(depth-1).
     * Uses identity jq expression (.) so nodes produce values when data is uploaded.
     */
    public void buildChainInGroup(long groupId, long rootId, int depth) throws Exception {
        long previousId = rootId;
        int created = 0;
        while (created < depth) {
            tm.begin();
            NodeGroupEntity g = NodeGroupEntity.findById(groupId);
            int batchEnd = Math.min(created + BATCH_SIZE, depth);
            for (int i = created; i < batchEnd; i++) {
                NodeEntity previous = NodeEntity.findById(previousId);
                JqNode node = new JqNode("n_" + i, ".", previous);
                node.group = g;
                NodeEntity merged = nodeService.create(node);
                previousId = merged.id;
            }
            tm.commit();
            created = batchEnd;
        }
    }

    /**
     * Creates a layered DAG (diamond/lattice topology) in an existing group.
     * Uses identity jq expression (.) so nodes produce values when data is uploaded.
     */
    public void buildDiamondInGroup(long groupId, long rootId, int layers, int width) throws Exception {
        List<Long> previousLayerIds = new ArrayList<>();

        // First layer: each node depends on root
        tm.begin();
        NodeGroupEntity g = NodeGroupEntity.findById(groupId);
        NodeEntity root = NodeEntity.findById(rootId);
        for (int w = 0; w < width; w++) {
            JqNode node = new JqNode("L0_n" + w, ".", root);
            node.group = g;
            NodeEntity merged = nodeService.create(node);
            previousLayerIds.add(merged.id);
        }
        tm.commit();

        // Subsequent layers: each node depends on ALL nodes in previous layer
        for (int layer = 1; layer < layers; layer++) {
            List<Long> currentLayerIds = new ArrayList<>();
            tm.begin();
            g = NodeGroupEntity.findById(groupId);
            List<NodeEntity> sources = new ArrayList<>();
            for (Long prevId : previousLayerIds) {
                sources.add(NodeEntity.findById(prevId));
            }
            for (int w = 0; w < width; w++) {
                JqNode node = new JqNode("L" + layer + "_n" + w, ".", sources);
                node.group = g;
                NodeEntity merged = nodeService.create(node);
                currentLayerIds.add(merged.id);
            }
            tm.commit();
            previousLayerIds = currentLayerIds;
        }
    }
}
