package io.hyperfoil.tools.h5m.benchmark;

import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@State(Scope.Thread)
public class ValueDependsOnBenchmark {

    @State(Scope.Thread)
    public static class ValueChainState {
        @Param({"10", "100", "1000"})
        int depth;

        ValueEntity[] values;
        ValueEntity leaf;
        ValueEntity root;
        ValueEntity middle;

        @Setup(Level.Trial)
        public void setup() {
            GraphBuilder.resetIds();
            JqNode[] nodeChain = GraphBuilder.buildDeepChain(depth);
            values = GraphBuilder.buildValueChain(nodeChain);
            root = values[0];
            leaf = values[depth - 1];
            middle = values[depth / 2];
        }
    }

    @Benchmark
    public boolean valueChain_dependsOn_root(ValueChainState state) {
        return state.leaf.dependsOn(state.root);
    }

    @Benchmark
    public boolean valueChain_dependsOn_middle(ValueChainState state) {
        return state.leaf.dependsOn(state.middle);
    }

    @Benchmark
    public boolean valueChain_dependsOn_notFound(ValueChainState state) {
        return state.root.dependsOn(state.leaf);
    }
}
