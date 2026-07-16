package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.h5m.entity.node.FingerprintNode;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RelativeDifference;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.h5m.entity.work.Work;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class WorkTest {


    @Test
    public void hashCode_relativeDifference(){
        NodeEntity rootNode = new JqNode("root",".root");
        NodeEntity relativeDifference = new RelativeDifference();
        ValueEntity rootValue1 = new ValueEntity(null,rootNode,JqValues.parse("\"text1\""));
        rootValue1.id = 1L;
        ValueEntity rootValue2 = new ValueEntity(null,rootNode,JqValues.parse("\"text2\""));
        rootValue2.id = 2L;

        Work work1 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue1.id));
        Work work2 = new Work(relativeDifference,List.of(rootNode),List.of(rootValue2.id));

        assertNotEquals(work1.hashCode(),work2.hashCode(),"both work should not have the same hash code due to different values");
        assertNotEquals(work1,work2,"work1 and work2 should not be considered equal due to different source values");
    }

    @Test
    public void hashCode_jqNode_same_parameters(){
        NodeEntity rootNode = new RootNode();
        NodeEntity activeNode = new JqNode("active");
        activeNode.sources=List.of(rootNode);

        ValueEntity rootValue = new ValueEntity(null,rootNode,JqValues.parse("\"root\""));
        rootValue.id = 1L;

        Work work1 = new Work(activeNode,activeNode.sources,List.of(rootValue.id));
        Work work2 = new Work(activeNode,activeNode.sources,List.of(rootValue.id));

        assertTrue(work1.hashCode() == work2.hashCode(),"same activeNode, source node, source values should have same hashcode");
    }

    @Test
    public void hashCode_relativedifference_same_parameters(){
        NodeEntity rootNode = new RootNode();
        NodeEntity fingerprint1 = new JqNode("fingerprint");
        fingerprint1.sources = List.of(rootNode);
        NodeEntity fingerprint2 = new JqNode("fingerprint");
        fingerprint2.sources = List.of(rootNode);
        NodeEntity domain = new JqNode("domain");
        domain.sources = List.of(rootNode);
        NodeEntity range = new JqNode("range");
        range.sources = List.of(rootNode);

        ValueEntity rootValue = new ValueEntity(null,rootNode,JqValues.parse("\"root\""));
        rootValue.id = 1L;

        FingerprintNode fingerprintNode = new FingerprintNode();
        fingerprintNode.sources = List.of(fingerprint1,fingerprint2);
        RelativeDifference rd = new RelativeDifference();
        rd.sources = List.of(fingerprintNode,range,domain);

        Work work1 = new Work(rd,rd.sources,List.of(rootValue.id));
        Work work2 = new Work(rd,rd.sources,List.of(rootValue.id));

        assertTrue(work1.hashCode() == work2.hashCode(),"same activeNode, source node, source values should have same hashcode");

    }

    @Test
    public void dependsOn_node_dependency_no_value(){
        NodeEntity one = new JqNode("one");
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);

        Work wOne = new Work(one,null, null);
        Work wTwo = new Work(two,null, null);

        assertTrue(wTwo.dependsOn(wOne));
    }
    @Test
    public void dependsOn_node_dependency_same_value(){
        NodeEntity root = new JqNode("root");
        NodeEntity one = new JqNode("one");
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);

        ValueEntity value = new ValueEntity(null,root);
        value.id = 1L;

        Work wOne = new Work(one,null, List.of(value.id));
        Work wTwo = new Work(two,null, List.of(value.id));

        assertTrue(wTwo.dependsOn(wOne));
    }
    @Test
    public void dependsOn_node_dependency_different_value(){
        NodeEntity root = new JqNode("root");
        NodeEntity one = new JqNode("one");
        NodeEntity two = new JqNode("two");
        two.sources = List.of(one);

        ValueEntity value1 = new ValueEntity(null,root);
        value1.id = 1L;
        ValueEntity value2 = new ValueEntity(null,one);
        value2.id = 2L;

        Work wOne = new Work(one,null, List.of(value1.id));
        Work wTwo = new Work(two,null, List.of(value2.id));

        assertFalse(wTwo.dependsOn(wOne));
    }
}
