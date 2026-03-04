package io.hyperfoil.tools.h5m.benchmark;

import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.queue.KahnDagSort;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Thread)
public class KahnDagSortBenchmark {

    // --- Flat list (zero edges) ---

    @State(Scope.Thread)
    public static class FlatState {
        @Param({"10", "100", "500"})
        int size;

        List<JqNode> nodes;

        @Setup(Level.Trial)
        public void setup() {
            GraphBuilder.resetIds();
            nodes = new ArrayList<>(Arrays.asList(GraphBuilder.buildFlatList(size)));
        }
    }

    @Benchmark
    public List<JqNode> sort_flat(FlatState state) {
        return KahnDagSort.sort(state.nodes, n -> (List<JqNode>) (List<?>) n.getSources());
    }

    // --- Chain in reversed order ---

    @State(Scope.Thread)
    public static class ChainState {
        @Param({"10", "100", "500"})
        int depth;

        List<JqNode> reversed;

        @Setup(Level.Trial)
        public void setup() {
            GraphBuilder.resetIds();
            JqNode[] chain = GraphBuilder.buildDeepChain(depth);
            reversed = new ArrayList<>(Arrays.asList(chain));
            Collections.reverse(reversed);
        }
    }

    @Benchmark
    public List<JqNode> sort_chain_reversed(ChainState state) {
        return KahnDagSort.sort(state.reversed, n -> (List<JqNode>) (List<?>) n.getSources());
    }

    // --- Layered DAG ---

    @State(Scope.Thread)
    public static class LayeredDagState {
        @Param({"3", "5", "8"})
        int layers;

        @Param({"5", "10"})
        int nodesPerLayer;

        List<JqNode> shuffled;

        @Setup(Level.Trial)
        public void setup() {
            GraphBuilder.resetIds();
            shuffled = GraphBuilder.buildLayeredDag(layers, nodesPerLayer);
        }
    }

    @Benchmark
    public List<JqNode> sort_layeredDag(LayeredDagState state) {
        return KahnDagSort.sort(state.shuffled, n -> (List<JqNode>) (List<?>) n.getSources());
    }

    // --- isCircular benchmarks ---

    @State(Scope.Thread)
    public static class CircularState {
        @Param({"10", "100", "500"})
        int size;

        JqNode acyclicHead;
        JqNode cyclicHead;

        @Setup(Level.Trial)
        public void setup() {
            // Acyclic chain
            GraphBuilder.resetIds();
            JqNode[] chain = GraphBuilder.buildDeepChain(size);
            acyclicHead = chain[size - 1];

            // Cyclic: same chain but last node also points back to first
            GraphBuilder.resetIds();
            JqNode[] cyclicChain = GraphBuilder.buildDeepChain(size);
            cyclicChain[0].sources.add(cyclicChain[size - 1]);
            cyclicHead = cyclicChain[size - 1];
        }
    }

    @Benchmark
    public boolean isCircular_acyclic(CircularState state) {
        return KahnDagSort.isCircular(state.acyclicHead, n -> (List<JqNode>) (List<?>) n.getSources());
    }

    @Benchmark
    public boolean isCircular_cyclic(CircularState state) {
        return KahnDagSort.isCircular(state.cyclicHead, n -> (List<JqNode>) (List<?>) n.getSources());
    }
}
