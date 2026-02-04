package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class NodeTest {

    @Inject
    EntityManager em;

    @Test
    public void compareTo(){
        Node n1 = new JqNode("n1",".");
        Node n2 = new JqNode("n2",".",n1);
        Node n3 = new JqNode("n3",".'");

        assertEquals(-1,n1.compareTo(n2),"n1 is before n2 because n2 depends on 1");
        assertEquals(1,n2.compareTo(n1),"n2 is after n1 because n2 depends on n1");
        assertEquals(-1,n3.compareTo(n2),"n3 is before n2 because it n3 does not have source");
        assertEquals(1,n2.compareTo(n3),"n2 is after n3 because it n3 does not have source");
        assertEquals(0,n1.compareTo(n3),"n1 and n3 are considered equal");
        assertEquals(0,n3.compareTo(n1),"n1 and n3 are considered equal");
    }

    @Test
    public void khanDagSort(){
        Node n1 = new JqNode("n1",".");
        Node n11 = new JqNode("n1.1",".",n1);
        Node n12 = new JqNode("n1.2",".",n1);
        Node n2 = new JqNode("n2",".");
        Node n121 = new JqNode("n1.2.1",".",n12,n2);

        List<Node> sorted = Node.kahnDagSort(List.of(n121,n12,n2,n1,n11));
        int n1Index = sorted.indexOf(n1);
        int n11Index = sorted.indexOf(n11);
        int n12Index = sorted.indexOf(n12);
        int n121Index = sorted.indexOf(n121);
        int n2Index = sorted.indexOf(n2);


        assertTrue(n1Index < n11Index,"n1 before n1.1");
        assertTrue(n1Index < n12Index,"n1 before n1.2");
        assertTrue(n1Index < n121Index,"n1 before n1.2.1");
        assertTrue(n2Index < n121Index,"n2 before n1.2.1");

    }

    //we want the sorting to be stable so user control the order
    @Test
    public void khanDagSort_is_stable(){
        Node n1 = new JqNode("n1");
        Node n2 = new JqNode("n2");
        Node n3 = new JqNode("n3");
        Node n4 = new JqNode("n4");
        Node n5 = new JqNode("n5");
        Node n6 = new JqNode("n6");

        List<Node> input = List.of(n4,n3,n5,n1,n6,n2);
        List<Node> sorted = Node.kahnDagSort(input);

        assertEquals(input.size(), sorted.size(),"sorting should return the same number of elements");
        for(int i=0; i<input.size();i++){
            assertEquals(input.get(i),sorted.get(i),"khanDagSort should be stable if changes are not necessary but "+i+" changed\ninput: "+input+"\nsorted: "+sorted);
        }
    }


    @Test
    public void isCircular_not_circular(){
        Node n1 = new JqNode("n1");
        Node n11 = new JqNode("n1.1",".",n1);
        Node n12 = new JqNode("n1.2",".",n1);
        Node n2 = new JqNode("n2");
        Node n121 = new JqNode("n1.2.1",".",n12,n2);

        assertFalse(n1.isCircular(),"a tree should not be considered circular");
        assertFalse(n11.isCircular(),"a tree should not be considered circular");
        assertFalse(n121.isCircular(),"a tree should not be considered circular");
    }
    @Test
    public void isCircular_circular_three_nodes(){
        Node n1 = new JqNode("n1",".");
        Node n2 = new JqNode("n2",".",n1);
        Node n3 = new JqNode("n3",".",n2);
        n1.sources=List.of(n3);

        assertTrue(n1.isCircular(),"n1 should be circular");
        assertTrue(n2.isCircular(),"n2 should be circular");
        assertTrue(n3.isCircular(),"n3 should be circular");
    }
    @Test
    public void isCircular_circular_self(){
        Node n1 = new JqNode("n1");
        n1.sources=List.of(n1);
        assertTrue(n1.isCircular(),"n1 should be circular");
    }


}
