package io.hyperfoil.tools.h5m.benchmark;

import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class GraphBuilder {

    private static final AtomicLong idGen = new AtomicLong(1);

    public static void resetIds() {
        idGen.set(1);
    }

    private static JqNode newNode(String name) {
        JqNode node = new JqNode(name);
        node.id = idGen.getAndIncrement();
        return node;
    }

    private static ValueEntity newValue(NodeEntity node) {
        ValueEntity v = new ValueEntity();
        v.id = idGen.getAndIncrement();
        v.node = node;
        return v;
    }

    /**
     * Linear chain: n0 -> n1 -> ... -> n(depth-1)
     * Each node's sources list contains the previous node.
     */
    public static JqNode[] buildDeepChain(int depth) {
        JqNode[] nodes = new JqNode[depth];
        nodes[0] = newNode("n0");
        for (int i = 1; i < depth; i++) {
            nodes[i] = newNode("n" + i);
            nodes[i].sources.add(nodes[i - 1]);
        }
        return nodes;
    }

    /**
     * One root with 'width' children.
     * Each child's sources list contains the root.
     */
    public static JqNode[] buildWideFan(int width) {
        JqNode[] nodes = new JqNode[width + 1];
        nodes[0] = newNode("root");
        for (int i = 1; i <= width; i++) {
            nodes[i] = newNode("leaf" + i);
            nodes[i].sources.add(nodes[0]);
        }
        return nodes;
    }

    /**
     * Diamond/lattice: root -> (layers-2) middle layers of 'width' nodes each -> sink.
     * Every node in layer L depends on all nodes in layer L-1.
     */
    public static JqNode[] buildDiamond(int layers, int width) {
        List<JqNode> all = new ArrayList<>();
        // Layer 0: root
        JqNode root = newNode("root");
        all.add(root);
        List<JqNode> prevLayer = List.of(root);

        // Middle layers
        for (int l = 1; l < layers - 1; l++) {
            List<JqNode> currentLayer = new ArrayList<>();
            for (int w = 0; w < width; w++) {
                JqNode node = newNode("L" + l + "_" + w);
                node.sources.addAll(prevLayer);
                currentLayer.add(node);
                all.add(node);
            }
            prevLayer = currentLayer;
        }

        // Last layer: sink
        JqNode sink = newNode("sink");
        sink.sources.addAll(prevLayer);
        all.add(sink);

        return all.toArray(new JqNode[0]);
    }

    /**
     * Build a ValueEntity chain mirroring a node chain.
     * v[i] has v[i-1] as its source.
     */
    public static ValueEntity[] buildValueChain(JqNode[] nodeChain) {
        ValueEntity[] values = new ValueEntity[nodeChain.length];
        values[0] = newValue(nodeChain[0]);
        for (int i = 1; i < nodeChain.length; i++) {
            values[i] = newValue(nodeChain[i]);
            values[i].sources.add(values[i - 1]);
        }
        return values;
    }

    /**
     * Independent nodes with no edges (for sort baseline).
     */
    public static JqNode[] buildFlatList(int size) {
        JqNode[] nodes = new JqNode[size];
        for (int i = 0; i < size; i++) {
            nodes[i] = newNode("flat" + i);
        }
        return nodes;
    }

    /**
     * Layered DAG: each node in layer L depends on all nodes in layer L-1.
     * Returns the list shuffled so sort has work to do.
     */
    public static List<JqNode> buildLayeredDag(int layers, int nodesPerLayer) {
        List<JqNode> all = new ArrayList<>();
        List<JqNode> prevLayer = new ArrayList<>();

        for (int l = 0; l < layers; l++) {
            List<JqNode> currentLayer = new ArrayList<>();
            for (int n = 0; n < nodesPerLayer; n++) {
                JqNode node = newNode("L" + l + "_" + n);
                node.sources.addAll(prevLayer);
                currentLayer.add(node);
                all.add(node);
            }
            prevLayer = currentLayer;
        }

        Collections.shuffle(all);
        return all;
    }
}
