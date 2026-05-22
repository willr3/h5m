package io.hyperfoil.tools.h5m.entity;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.api.NodeGroup;
import io.hyperfoil.tools.h5m.api.NodeType;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.JsNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.hyperfoil.tools.yaup.HashedSets;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.hibernate.query.NativeQuery;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class NodeTest extends FreshDb {

    @Inject
    EntityManager em;

    @Inject
    TransactionManager tm;


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

    record ClosureEntry (long parentId,long childId,int idx,int depth,int count){};
    private List<ClosureEntry> loadClosureTable(){
        List<ClosureEntry> rtrn = new ArrayList<>();
        NativeQuery query = (NativeQuery) em.createNativeQuery("select parent_id,child_id,idx,depth,count from node_edge order by parent_id asc");
        List<Object[]> found = query
                .unwrap(NativeQuery.class)
                .addScalar("parent_id", Long.class)
                .addScalar("child_id", Long.class)
                .addScalar("idx", Integer.class)
                .addScalar("depth", Integer.class)
                .addScalar("count", Integer.class)
                .getResultList();
        for(int i=0; i<found.size();i++) {
            Object[] obj = found.get(i);
            Long parentId = (Long) obj[0];
            Long childId = (Long) obj[1];
            Integer idx = (Integer) obj[2];
            Integer depth = (Integer) obj[3];
            Integer count = (Integer) obj[4];
            rtrn.add(new ClosureEntry(parentId,childId,idx,depth,count));
        }
        return rtrn;
    }
    private HashedSets<Long,Long> loadClosureReferences(){
        HashedSets<Long,Long> map = new HashedSets<>();
        List<ClosureEntry> table = loadClosureTable();
        table.stream().filter(ce->ce.count>0).forEach(e -> {map.put(e.parentId,e.childId);});
        return map;
    }

    private String printClosureTable(){
        StringBuilder sb = new StringBuilder();
        List<ClosureEntry> table = loadClosureTable();
        sb.append("-- closure table --\n");

        table.forEach((c)->{
            NodeEntity parent = NodeEntity.findById(c.parentId);
            NodeEntity child = NodeEntity.findById(c.childId);
            sb.append(parent.name+"("+parent.id+") -> "+child.name+"("+child.id+") idx="+c.idx+" depth="+c.depth+" count="+c.count+"\n");
        });
        return sb.toString();
    }



    @Test
    public void closure_create_self_reference() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode();
        root.name="root";
        root.persist();
        tm.commit();

        HashedSets<Long,Long> edges = loadClosureReferences();

        assertEquals(1,edges.size(),"expect a self reference in closure table");
        assertTrue(edges.has(root.id),"expect to find root in edges");
        assertEquals(1,edges.get(root.id).size(),"root should only have one reference");
        assertTrue(edges.get(root.id).contains(root.id),"root should self reference");
    }

    @Test
    public void closure_create_child_reference() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.name="root";
        root.persist();
        NodeEntity a1 = new JsNode("a1",".a1");
        a1.persist();
        a1.sources=List.of(root);
        a1.persist();
        tm.commit();



        HashedSets<Long,Long> table = loadClosureReferences();

        assertNotNull(table);
        assertFalse(table.isEmpty());

        assertEquals(2,table.size(),"expect two top level entries for table");
        assertTrue(table.has(root.id),"expect to find root in table");
        assertEquals(2,table.get(root.id).size(),"root should have 2 references");
        assertTrue(table.get(root.id).contains(root.id),"root should self reference: "+table.get(root.id));
        assertTrue(table.get(root.id).contains(a1.id),"root should have a1 reference");
        assertTrue(table.has(a1.id),"expect to find a1 in table");
        assertEquals(1,table.get(a1.id).size(),"a1 should have 1 reference");
        assertTrue(table.get(a1.id).contains(a1.id),"a1 should self reference");

        List<ClosureEntry> entries =  loadClosureTable();
        assertEquals(1,entries.stream().mapToInt(e->e.count).max().orElse(-1),"max count is 1");
    }

    @Test
    public void closure_create_grandchild_reference() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.name="root";
        root.persist();
        NodeEntity a1 = new JsNode("a1",".a1",List.of(root));
        a1.persist();
        NodeEntity a2 = new JsNode("a2",".a2",List.of(a1));
        a2.persist();
        tm.commit();

        HashedSets<Long,Long> table = loadClosureReferences();
        List<ClosureEntry> entries = loadClosureTable();

        assertNotNull(table);
        assertFalse(table.isEmpty());
        assertTrue(table.has(root.id),"expect to find root in table");
        assertEquals(3,table.get(root.id).size(),"root should have 3 references");
        assertTrue(table.get(root.id).contains(root.id),"root should self reference");
        assertTrue(table.get(root.id).contains(a1.id),"root should have a1 reference");
        int rootToA1Depth = entries.stream().filter(e->e.parentId==root.id && e.childId==a1.id).map(e->e.depth()).findFirst().orElse(-1);
        assertEquals(1,rootToA1Depth,"a1 should be 1 deep from root");
        assertTrue(table.get(root.id).contains(a2.id),"root should have a2 reference");
        int rootToA2Depth = entries.stream().filter(e->e.parentId==root.id && e.childId==a2.id).map(e->e.depth()).findFirst().orElse(-1);
        assertEquals(2,rootToA2Depth,"a2 should be 2 deep from root");
        assertTrue(table.has(a1.id),"expect to find a1 in table");
        assertEquals(2,table.get(a1.id).size(),"a1 should have 2 references");
        assertTrue(table.get(a1.id).contains(a1.id),"a1 should self reference");
        assertTrue(table.get(a1.id).contains(a2.id),"a1 should have a2 reference");

        assertEquals(1,entries.stream().mapToInt(e->e.count).max().orElse(-1),"max count is 1");

    }
    @Test
    public void closure_increase_count_for_shared_reference() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.name="root";
        root.persist();
        NodeEntity a1 = new JsNode("a1",".a1",List.of(root));
        a1.persist();
        NodeEntity b1 = new JsNode("b1",".b2",List.of(root));
        b1.persist();
        NodeEntity ab = new JsNode("ab",".ab",List.of(a1,b1));
        ab.persist();
        tm.commit();

        List<ClosureEntry> entries = loadClosureTable();

        ClosureEntry rootToAB = entries.stream().filter(e->e.parentId==root.id && e.childId==ab.id).findFirst().orElse(null);
        assertNotNull(rootToAB,"path should exist from root to ab");
        assertEquals(2,rootToAB.count,"root should have 2 paths to it from root");

        ClosureEntry a1ToAB = entries.stream().filter(e->e.parentId==a1.id && e.childId==ab.id).findFirst().orElse(null);
        assertNotNull(a1ToAB,"path should exist from a1 to ab");
        assertEquals(0,a1ToAB.idx,"a1 to ab should be at index=0");
        ClosureEntry b1ToAB = entries.stream().filter(e->e.parentId==b1.id && e.childId==ab.id).findFirst().orElse(null);
        assertNotNull(b1ToAB,"path should exist from b1 to ab");
        assertEquals(1,b1ToAB.count,"b1 to ab should be at index=1");
    }


    @Test
    public void closure_delete_not_remove_shared_grandchild() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        /*
              root
           ┌──┼──┐
           a1 │  b1
           a2 │  b2
           └──┼──┚
              ab
         */
        root.name="root";
        root.persist();
        NodeEntity a1 = new JsNode("a1",".a1",List.of(root));
        a1.persist();
        NodeEntity a2 = new JsNode("a2",".a2",List.of(a1));
        a2.persist();
        NodeEntity b1 = new JqNode("b1",".b1",List.of(root));
        b1.persist();
        NodeEntity b2 = new JqNode("b2",".b2",List.of(b1));
        b2.persist();
        NodeEntity ab = new JqNode("ab",".ab",List.of(a2,b2,root));
        ab.persist();
        tm.commit();

        HashedSets<Long,Long> map = loadClosureReferences();

        assertTrue(map.has(root.id),"table should contain root");
        assertTrue(map.get(root.id).contains(ab.id),"root should reach ab");
        assertTrue(map.has(a1.id),"table should contain a1");
        assertTrue(map.get(a1.id).contains(ab.id),"a1 should reach ab");
        assertTrue(map.has(a2.id),"table should contain a2");
        assertTrue(map.get(a2.id).contains(ab.id),"a2 should reach ab");
        assertTrue(map.has(b1.id),"table should contain b1");
        assertTrue(map.get(b1.id).contains(ab.id),"b1 should reach ab");
        assertTrue(map.has(b2.id),"table should contain b2");
        assertTrue(map.get(b2.id).contains(ab.id),"b2 should reach ab");
        assertTrue(map.has(ab.id),"table should contain ab");
        assertTrue(map.get(ab.id).contains(ab.id),"ab should reach ab");

        tm.begin();
        NodeEntity fetchAB = NodeEntity.findById(ab.id);
        NodeEntity fetchA2 = NodeEntity.findById(a2.id);
        assertTrue(fetchAB.sources.contains(fetchA2),"fetched ab should contain a2 source");
        int idx = fetchAB.sources.indexOf(fetchA2);
        fetchAB.sources.remove(idx);
        fetchAB.persist();
        tm.commit();

        /*
              root
           ┌──┼──┐
           a1 │  b1
           a2 │  b2
              ├──┚
              ab
        */
        map = loadClosureReferences();
        assertTrue(map.has(root.id),"table should contain root");
        assertTrue(map.get(root.id).contains(ab.id),"root should reach ab");
        assertTrue(map.has(a1.id),"table should contain a1");
        assertFalse(map.get(a1.id).contains(ab.id),"a1 should not reach ab\n"+printClosureTable());
        assertTrue(map.has(a2.id),"table should contain a2");
        assertFalse(map.get(a2.id).contains(ab.id),"a2 should not reach ab");
        assertTrue(map.has(b1.id),"table should contain b1");
        assertTrue(map.get(b1.id).contains(ab.id),"b1 should reach ab");
        assertTrue(map.has(b2.id),"table should contain b2");
        assertTrue(map.get(b2.id).contains(ab.id),"b2 should reach ab");
        assertTrue(map.has(ab.id),"table should contain ab");
        assertTrue(map.get(ab.id).contains(ab.id),"ab should reach ab");
    }

    @Test
    public void closure_delete_not_remove_shared_child() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.name = "root";
        root.persist();
        NodeEntity n1 = new JqNode("n1",".n1",root);
        n1.persist();
        NodeEntity n2 = new JqNode("n2",".n2",root);
        n2.persist();
        NodeEntity n3 = new JqNode("n3",".n3",List.of(n1,n2));
        n3.persist();
        tm.commit();

        HashedSets<Long,Long> map = loadClosureReferences();
        assertEquals(4,map.size());
        assertTrue(map.has(root.id),"root node should be in closure table");
        assertEquals(4,map.get(root.id).size(),"root node should have 4 entries: "+map.get(root.id));
        assertTrue(map.get(root.id).containsAll(List.of(root.id,n1.id,n2.id,n3.id)),"root missing an expected closure entry: "+map.get(root.id));
        assertTrue(map.has(n1.id),"n1 should be in closure table");
        assertEquals(2,map.get(n1.id).size(),"n1 should have 2 entries: "+map.get(n1.id));
        assertTrue(map.get(n1.id).containsAll(List.of(n1.id,n3.id)),"n1 missing an expected closure entry: "+map.get(n1.id));
        assertTrue(map.has(n2.id),"n2 should be in closure table");
        assertEquals(2,map.get(n2.id).size(),"n2 should have 2 entries: "+map.get(n2.id));
        assertTrue(map.get(n2.id).containsAll(List.of(n2.id,n3.id)),"n2 missing an expected closure entry: "+map.get(n2.id));
        assertTrue(map.has(n3.id),"n3 should be in closure table");
        assertEquals(1,map.get(n3.id).size(),"n3 should have 1 entry: "+map.get(n3.id));
        assertTrue(map.get(n3.id).contains(n3.id),"n3 missing an expected closure entry: "+map.get(n3.id));

        //removing n1 from n3 sources

        tm.begin();
        NodeEntity fetchN1 = NodeEntity.findById(n1.getId());
        assertNotNull(fetchN1);
        NodeEntity fetchN3 = NodeEntity.findById(n3.getId());
        assertNotNull(fetchN3);
        assertTrue(fetchN3.sources.contains(fetchN1));
        fetchN3.sources.remove(fetchN1);
        fetchN3.persist();
        tm.commit();

        map = loadClosureReferences();
        assertEquals(4,map.size());
        assertTrue(map.has(root.id),"root node should be in closure table");
        assertEquals(4,map.get(root.id).size(),"root node should have 4 entries: "+map.get(root.id)+" expecting "+root.id+", "+n1.id+", "+n2.id+", "+n3.id);
        assertTrue(map.get(root.id).containsAll(List.of(root.id,n1.id,n2.id,n3.id)),"root missing an expected closure entry: "+map.get(root.id));
        assertTrue(map.has(n1.id),"n1 should be in closure table");
        assertEquals(1,map.get(n1.id).size(),"n1 should have 2 entries: "+map.get(n1.id));
        assertTrue(map.get(n1.id).containsAll(List.of(n1.id)),"n1 missing an expected closure entry: "+map.get(n1.id));
        assertTrue(map.has(n2.id),"n2 should be in closure table");
        assertEquals(2,map.get(n2.id).size(),"n2 should have 2 entries: "+map.get(n2.id));
        assertTrue(map.get(n2.id).containsAll(List.of(n2.id,n3.id)),"n2 missing an expected closure entry: "+map.get(n2.id));
        assertTrue(map.has(n3.id),"n3 should be in closure table");
        assertEquals(1,map.get(n3.id).size(),"n3 should have 1 entry: "+map.get(n3.id));
        assertTrue(map.get(n3.id).contains(n3.id),"n3 missing an expected closure entry: "+map.get(n3.id));

    }

    @Test
    public void create_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        tm.commit();
    }

    @Test
    public void multi_generation_dependency() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        try {
            tm.begin();
            NodeEntity root = new RootNode();
            root.persist();
            NodeEntity a = new JqNode("a");
            a.sources = List.of(root);
            a.persist();
            NodeEntity ab = new JqNode("ab");
            ab.sources = List.of(a);
            ab.persist();
            NodeEntity ac = new JqNode("ac");
            ac.sources = List.of(a);
            ac.persist();
            NodeEntity abc = new JqNode("abc");
            abc.sources = List.of(ab, ac );
            abc.persist();
            tm.commit();

            NodeEntity found = NodeEntity.findById(abc.getId());
            assertNotNull(found);

            System.out.println(found.sources);

            assertEquals(2,found.sources.size(),"expect to find 2 sources: "+found.sources);

        }catch(Exception e){
            fail(e.getMessage(),e);
        }


    }
}
