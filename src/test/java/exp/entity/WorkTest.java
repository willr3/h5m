package exp.entity;

import exp.entity.node.JqNode;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
public class WorkTest {

    @Test
    public void dependsOn_node_dependency_no_value(){
        Node one = new JqNode("one");
        Node two = new JqNode("two");
        two.sources = List.of(one);

        Work wOne = new Work(one,null, null);
        Work wTwo = new Work(two,null, null);

        assertTrue(wTwo.dependsOn(wOne));
    }
    @Test
    public void dependsOn_node_dependency_same_value(){
        Node root = new JqNode("root");
        Node one = new JqNode("one");
        Node two = new JqNode("two");
        two.sources = List.of(one);

        Value value = new Value(null,root);

        Work wOne = new Work(one,null, List.of(value));
        Work wTwo = new Work(two,null, List.of(value));

        assertTrue(wTwo.dependsOn(wOne));
    }
    @Test
    public void dependsOn_node_dependency_different_value(){
        Node root = new JqNode("root");
        Node one = new JqNode("one");
        Node two = new JqNode("two");
        two.sources = List.of(one);

        Value value1 = new Value(null,root);
        Value value2 = new Value(null,one);

        Work wOne = new Work(one,null, List.of(value1));
        Work wTwo = new Work(two,null, List.of(value2));

        assertFalse(wTwo.dependsOn(wOne));
    }
}
