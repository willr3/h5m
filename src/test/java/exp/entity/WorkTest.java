package exp.entity;

import com.fasterxml.jackson.databind.node.TextNode;
import exp.entity.node.FingerprintNode;
import exp.entity.node.JqNode;
import exp.entity.node.RelativeDifference;
import exp.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class WorkTest {


    @Test
    public void hashCode_relativeDifference(){
        Node rootNode = new JqNode("root",".root");
        Node relativeDifference = new RelativeDifference();
        Value rootValue1 = new Value(null,rootNode,new TextNode("text1"));
        Value rootValue2 = new Value(null,rootNode,new TextNode("text2"));

        Work work1 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue1));
        Work work2 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue2));

        assertEquals(work1.hashCode(),work2.hashCode(),"both work should have the same hash code despite different values");
        assertEquals(work1,work2,"work1 and work2 should be considered equal despite different source values");
    }

    @Test
    public void hashCode_jqNode_same_parameters(){
        Node rootNode = new RootNode();
        Node activeNode = new JqNode("active");
        activeNode.sources=List.of(rootNode);

        Value rootValue = new Value(null,rootNode,new TextNode("root"));

        Work work1 = new Work(activeNode,activeNode.sources,List.of(rootValue));
        Work work2 = new Work(activeNode,activeNode.sources,List.of(rootValue));

        assertTrue(work1.hashCode() == work2.hashCode(),"same activeNode, source node, source values should have same hashcode");
    }

    @Test
    public void hashCode_relativedifference_same_parameters(){
        Node rootNode = new RootNode();
        Node fingerprint1 = new JqNode("fingerprint");
        fingerprint1.sources = List.of(rootNode);
        Node fingerprint2 = new JqNode("fingerprint");
        fingerprint2.sources = List.of(rootNode);
        Node domain = new JqNode("domain");
        domain.sources = List.of(rootNode);
        Node range = new JqNode("range");
        range.sources = List.of(rootNode);

        Value rootValue = new Value(null,rootNode,new TextNode("root"));

        FingerprintNode fingerprintNode = new FingerprintNode();
        fingerprintNode.sources = List.of(fingerprint1,fingerprint2);
        RelativeDifference rd = new RelativeDifference();
        rd.sources = List.of(fingerprintNode,range,domain);

        Work work1 = new Work(rd,rd.sources,List.of(rootValue));
        Work work2 = new Work(rd,rd.sources,List.of(rootValue));

        assertTrue(work1.hashCode() == work2.hashCode(),"same activeNode, source node, source values should have same hashcode");

    }

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
