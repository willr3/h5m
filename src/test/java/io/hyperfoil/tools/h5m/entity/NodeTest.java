package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.api.EphemeralMode;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class NodeTest {

    @Inject
    EntityManager em;

    @Inject
    TransactionManager tm;

    @Test
    public void setEphemeral(){
        NodeEntity node = new JqNode();
        assertNotEquals(EphemeralMode.DISCARD,node.ephemeral,"node should not be ephemeral");

        node.ephemeral = EphemeralMode.DISCARD;

        assertEquals(EphemeralMode.DISCARD,node.ephemeral,"node should set to ephemeral");    }

    @Test
    public void root_setEphemeral(){
        NodeEntity node = new RootNode();

        assertNotEquals(EphemeralMode.DISCARD,node.ephemeral,"root node should not be ephemeral");
        assertEquals(EphemeralMode.KEEP,node.ephemeral,"root node should set to KEEP");
        node.ephemeral = EphemeralMode.DISCARD;

        assertNotEquals(EphemeralMode.DISCARD,node.ephemeral,"root node should not set to ephemeral");


    }

    @Test
    public void compareTo(){
        NodeEntity n1 = new JqNode("n1",".");
        NodeEntity n2 = new JqNode("n2",".",n1);
        NodeEntity n3 = new JqNode("n3",".'");

        assertEquals(-1,n1.compareTo(n2),"n1 is before n2 because n2 depends on 1");
        assertEquals(1,n2.compareTo(n1),"n2 is after n1 because n2 depends on n1");
        assertEquals(-1,n3.compareTo(n2),"n3 is before n2 because it n3 does not have source");
        assertEquals(1,n2.compareTo(n3),"n2 is after n3 because it n3 does not have source");
        assertEquals(0,n1.compareTo(n3),"n1 and n3 are considered equal");
        assertEquals(0,n3.compareTo(n1),"n1 and n3 are considered equal");
    }

    @Test
    public void khanDagSort(){
        NodeEntity n1 = new JqNode("n1",".");
        NodeEntity n11 = new JqNode("n1.1",".",n1);
        NodeEntity n12 = new JqNode("n1.2",".",n1);
        NodeEntity n2 = new JqNode("n2",".");
        NodeEntity n121 = new JqNode("n1.2.1",".",n12,n2);

        List<NodeEntity> sorted = NodeEntity.kahnDagSort(List.of(n121,n12,n2,n1,n11));
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
        NodeEntity n1 = new JqNode("n1");
        NodeEntity n2 = new JqNode("n2");
        NodeEntity n3 = new JqNode("n3");
        NodeEntity n4 = new JqNode("n4");
        NodeEntity n5 = new JqNode("n5");
        NodeEntity n6 = new JqNode("n6");

        List<NodeEntity> input = List.of(n4,n3,n5,n1,n6,n2);
        List<NodeEntity> sorted = NodeEntity.kahnDagSort(input);

        assertEquals(input.size(), sorted.size(),"sorting should return the same number of elements");
        for(int i=0; i<input.size();i++){
            assertEquals(input.get(i),sorted.get(i),"khanDagSort should be stable if changes are not necessary but "+i+" changed\ninput: "+input+"\nsorted: "+sorted);
        }
    }


    @Test
    public void isCircular_not_circular(){
        NodeEntity n1 = new JqNode("n1");
        NodeEntity n11 = new JqNode("n1.1",".",n1);
        NodeEntity n12 = new JqNode("n1.2",".",n1);
        NodeEntity n2 = new JqNode("n2");
        NodeEntity n121 = new JqNode("n1.2.1",".",n12,n2);

        assertFalse(n1.isCircular(),"a tree should not be considered circular");
        assertFalse(n11.isCircular(),"a tree should not be considered circular");
        assertFalse(n121.isCircular(),"a tree should not be considered circular");
    }
    @Test
    public void isCircular_circular_three_nodes(){
        NodeEntity n1 = new JqNode("n1",".");
        NodeEntity n2 = new JqNode("n2",".",n1);
        NodeEntity n3 = new JqNode("n3",".",n2);
        n1.sources=List.of(n3);

        assertTrue(n1.isCircular(),"n1 should be circular");
        assertTrue(n2.isCircular(),"n2 should be circular");
        assertTrue(n3.isCircular(),"n3 should be circular");
    }
    @Test
    public void isCircular_circular_self(){
        NodeEntity n1 = new JqNode("n1");
        n1.sources=List.of(n1);
        assertTrue(n1.isCircular(),"n1 should be circular");
    }


    @Test
    public void hashCode_not_infinite_recursion(){
        NodeGroupEntity group = new NodeGroupEntity("group");
        NodeEntity n = new JqNode("node","$.",group.root);
        group.sources.add(n);

        try {
            int hash = n.hashCode();
        }catch(StackOverflowError e){
            fail("infinite recursion in Node.hashCode");
        }
    }

    @Test
    public void equals_not_infinite_recursion(){
        NodeGroupEntity g1 = new NodeGroupEntity("group");
        NodeGroupEntity g2 = new NodeGroupEntity("group");

        NodeEntity n1 = new JqNode("node","$.");
        NodeEntity n2 = new JqNode("node","$.");

        g1.sources.add(n1);
        g2.sources.add(n2);

        try{
            n1.equals(n2);
            n1.equals(n1);
            g1.equals(g2);
            g1.equals(g1);
        }catch(StackOverflowError e){
            fail("infinite recursion in Node.equals");
        }
    }

    @Test
    public void dependsOn_parent_changes_sources() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity first = new JqNode("first",".",root);
        first.persist();
        NodeEntity second = new JqNode("second",".",first);
        second.persist();
        NodeEntity third = new JqNode("third",".",root);
        third.persist();

        assertFalse(second.dependsOn(third));
        tm.commit();

        tm.begin();
        root = NodeEntity.findById(root.getId());
        assertNotNull(root);
        first = NodeEntity.findById(first.getId());
        assertNotNull(first);
        second = NodeEntity.findById(second.getId());
        assertNotNull(second);
        third = NodeEntity.findById(third.getId());
        assertNotNull(third);

        assertFalse(second.dependsOn(third));

        first.sources=List.of(root,third);
        first.persist();

        assertTrue(second.dependsOn(third));

        tm.commit();






    }

}
