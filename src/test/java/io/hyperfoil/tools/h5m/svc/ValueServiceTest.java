package io.hyperfoil.tools.h5m.svc;

import io.hyperfoil.tools.jjq.value.*;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ValueServiceTest extends FreshDb {

    @Inject
    ValueService valueService;

    @Inject
    ApiMapper apiMapper;

    @Inject
    TransactionManager tm;

    @Inject
    EntityManager em;

    @Test
    public void delete_does_not_cascade_and_delete_ancestor() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        ValueEntity rootValue = new ValueEntity(null,rootNode,JqString.of("a"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,JqString.of("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        tm.commit();

        tm.begin();
        aValue.delete();
        tm.commit();

        ValueEntity found = ValueEntity.findById(rootValue.id);
        assertNotNull(found);
    }


    @Test
    public void delete_does_not_cascade_to_shared_child() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b");
        bNode.sources=List.of(rootNode);
        bNode.persist();

        ValueEntity rootValue = new ValueEntity(null,rootNode,JqString.of("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,JqString.of("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null,bNode,JqString.of("b"));
        bValue.sources=List.of(rootValue);
        bValue.persist();
        // X is shared: both aValue and bValue are parents
        ValueEntity xValue = new ValueEntity(null,aNode,JqString.of("x"));
        xValue.sources=List.of(aValue, bValue);
        xValue.persist();
        tm.commit();

        tm.begin();
        valueService.delete(aValue);
        tm.commit();

        // X should still exist because bValue is still a parent
        ValueEntity foundX = ValueEntity.findById(xValue.id);
        assertNotNull(foundX, "shared child X should not be deleted when one parent is removed");
        // bValue should still exist
        ValueEntity foundB = ValueEntity.findById(bValue.id);
        assertNotNull(foundB, "bValue should still exist");
    }

    @Test
    public void delete_cascades_to_exclusive_child() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();

        ValueEntity rootValue = new ValueEntity(null,rootNode,JqString.of("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,JqString.of("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        // X has only aValue as parent
        ValueEntity xValue = new ValueEntity(null,aNode,JqString.of("x"));
        xValue.sources=List.of(aValue);
        xValue.persist();
        tm.commit();

        tm.begin();
        valueService.delete(aValue);
        tm.commit();

        // X should be deleted because aValue was its only parent
        ValueEntity foundX = ValueEntity.findById(xValue.id);
        assertNull(foundX, "exclusive child X should be deleted when its only parent is removed");
    }

    @Test
    public void delete_mixed_shared_and_exclusive() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b");
        bNode.sources=List.of(rootNode);
        bNode.persist();

        ValueEntity rootValue = new ValueEntity(null,rootNode,JqString.of("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,JqString.of("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null,bNode,JqString.of("b"));
        bValue.sources=List.of(rootValue);
        bValue.persist();
        // X is shared between aValue and bValue
        ValueEntity xValue = new ValueEntity(null,aNode,JqString.of("x"));
        xValue.sources=List.of(aValue, bValue);
        xValue.persist();
        // Y is exclusive to aValue
        ValueEntity yValue = new ValueEntity(null,aNode,JqString.of("y"));
        yValue.sources=List.of(aValue);
        yValue.persist();
        tm.commit();

        tm.begin();
        valueService.delete(aValue);
        tm.commit();

        // X should survive (shared with bValue)
        ValueEntity foundX = ValueEntity.findById(xValue.id);
        assertNotNull(foundX, "shared child X should survive");
        // Y should be deleted (exclusive to aValue)
        ValueEntity foundY = ValueEntity.findById(yValue.id);
        assertNull(foundY, "exclusive child Y should be deleted");
    }

    @Test
    public void deleteDescendantValues_preserves_shared_descendants() throws Exception {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a", ".a");
        aNode.sources = List.of(rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b", ".b");
        bNode.sources = List.of(rootNode);
        bNode.persist();

        ValueEntity rootValue = new ValueEntity(null, rootNode, JqString.of("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null, aNode, JqString.of("a"));
        aValue.sources = List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null, bNode, JqString.of("b"));
        bValue.sources = List.of(rootValue);
        bValue.persist();
        // shared: child of both aValue and bValue
        ValueEntity sharedValue = new ValueEntity(null, aNode, JqString.of("shared"));
        sharedValue.sources = List.of(aValue, bValue);
        sharedValue.persist();
        // exclusive: child of aValue only
        ValueEntity exclusiveValue = new ValueEntity(null, aNode, JqString.of("exclusive"));
        exclusiveValue.sources = List.of(aValue);
        exclusiveValue.persist();
        tm.commit();

        tm.begin();
        int deleted = valueService.deleteDescendantValues(rootValue, aNode);
        // verify within same transaction using native SQL to bypass L1 cache
        long aCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", aValue.id).getSingleResult()).longValue();
        long exclusiveCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", exclusiveValue.id).getSingleResult()).longValue();
        long sharedCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", sharedValue.id).getSingleResult()).longValue();
        tm.commit();

        // aValue (parentCount=1) and exclusiveValue (parentCount=1) should be deleted
        // sharedValue (parentCount=2 from aValue+bValue) should survive
        assertEquals(0, aCount, "aValue should be deleted (single parent)");
        assertEquals(0, exclusiveCount, "exclusive descendant should be deleted");
        assertEquals(1, sharedCount, "shared descendant should survive");
        assertEquals(2, deleted, "aValue and exclusiveValue should be deleted");
    }

    @Test
    public void purge_removes_subtree_preserves_external() throws Exception {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a", ".a");
        aNode.sources = List.of(rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b", ".b");
        bNode.sources = List.of(rootNode);
        bNode.persist();

        ValueEntity rootValue = new ValueEntity(null, rootNode, JqString.of("root"));
        rootValue.persist();
        // aValue is the purge target
        ValueEntity aValue = new ValueEntity(null, aNode, JqString.of("a"));
        aValue.sources = List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null, bNode, JqString.of("b"));
        bValue.sources = List.of(rootValue);
        bValue.persist();
        // child only reachable via aValue
        ValueEntity childOfA = new ValueEntity(null, aNode, JqString.of("childOfA"));
        childOfA.sources = List.of(aValue);
        childOfA.persist();
        // shared child with external parent bValue
        ValueEntity sharedChild = new ValueEntity(null, aNode, JqString.of("shared"));
        sharedChild.sources = List.of(aValue, bValue);
        sharedChild.persist();
        tm.commit();

        tm.begin();
        int purged = valueService.purge(aValue);
        long aCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", aValue.id).getSingleResult()).longValue();
        long childCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", childOfA.id).getSingleResult()).longValue();
        long sharedCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", sharedChild.id).getSingleResult()).longValue();
        long bCount = ((Number) em.createNativeQuery("SELECT COUNT(*) FROM value WHERE id = :id")
                .setParameter("id", bValue.id).getSingleResult()).longValue();
        tm.commit();

        // aValue and childOfA should be gone
        assertEquals(0, aCount, "purge target should be deleted");
        assertEquals(0, childCount, "exclusive child should be deleted");
        // shared child and bValue should survive
        assertEquals(1, sharedCount, "shared child should survive (external parent bValue)");
        assertEquals(1, bCount, "bValue should survive");
        assertEquals(2, purged, "should delete aValue (root) + childOfA (exclusive) = 2");
    }

    @Test
    public void lazy_data() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqValues.parse("{\"this\":{ \"is\":{\"silly\":\"yes\"}}}"));
        rootValue01.persist();
        tm.commit();

        rootValue01 = null;

        List<ValueEntity> found = valueService.getValues(rootNode);
        assertEquals(1, found.size());
        // Data is available because findMultiple() serves from the 2LC
        // (the entity was cached when persisted above)
        for(ValueEntity v : found){
            assertNotNull(v.data, "data should be available from 2LC");
        }


    }

    @Test
    public void getAncestor_single_ancestor() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();
        NodeEntity abcNode = new JqNode("abc");
        abcNode.sources=List.of(abNode);
        abcNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqValues.parse("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqValues.parse("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqValues.parse("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,JqValues.parse("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,JqValues.parse("1"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,JqValues.parse("2"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();

        ValueEntity abcValue01 = new ValueEntity(null,abcNode,JqValues.parse("{\"a\":123,\"b\":456}"));//matching value to find fingerprint
        abcValue01.sources=List.of(abValue01);
        abcValue01.persist();
        ValueEntity abcValue02 = new ValueEntity(null,abcNode,JqValues.parse("{\"a\":123,\"b\":456}"));
        abcValue02.sources=List.of(abValue02);
        abcValue02.persist();
        tm.commit();

        List<ValueEntity> found =  valueService.getAncestor(abValue01,aNode);

        assertNotNull(found);
        assertEquals(1, found.size());
        assertTrue(found.contains(aValue01));
    }

    @Test
    public void getGroupedValues() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();

        NodeEntity bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqString.of("root01"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqString.of("root02"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqString.of("a01"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,JqString.of("a02"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,JqString.of("b01"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,JqString.of("b02"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();

        List<JqValue> jsons = valueService.getGroupedValues(rootNode.id);

        assertEquals(2,jsons.size(),"expect to find two entries: "+jsons);

        JqValue entry = jsons.get(0);
        assertNotNull(entry);
        assertInstanceOf(JqObject.class,entry);
        JqObject obj =  (JqObject) entry;
        assertTrue(obj.has("a"),"first entry should have a field: "+obj.toJsonString());
        assertTrue(obj.has("b"),"first entry should have b field: "+obj.toJsonString());
        entry = jsons.get(1);
        assertNotNull(entry);
        assertInstanceOf(JqObject.class,entry);
        obj =  (JqObject) entry;
        assertTrue(obj.has("a"),"second entry should have a field: "+obj.toJsonString());
        assertTrue(obj.has("b"),"second entry should have b field: "+obj.toJsonString());

        tm.commit();
    }

    @Test
    public void findMatchingFingerprint_deep_ancestry_json_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();
        NodeEntity abcNode = new JqNode("abc");
        abcNode.sources=List.of(abNode);
        abcNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqValues.parse("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqValues.parse("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqValues.parse("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,JqValues.parse("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,JqValues.parse("1"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,JqValues.parse("2"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();

        ValueEntity abcValue01 = new ValueEntity(null,abcNode,JqValues.parse("{\"a\":123,\"b\":456}"));//matching value to find fingerprint
        abcValue01.sources=List.of(abValue01);
        abcValue01.persist();
        ValueEntity abcValue02 = new ValueEntity(null,abcNode,JqValues.parse("{\"a\":123,\"b\":456}"));
        abcValue02.sources=List.of(abValue02);
        abcValue02.persist();
        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(aNode,rootNode,abcValue01,null,null,-1,-1,true);



        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(aValue01),found.toString());
        assertTrue(found.contains(aValue02),found.toString());

    }
    @Test
    public void findMatchingFingerprint_integer_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqValues.parse("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqValues.parse("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqValues.parse("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,JqValues.parse("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,JqValues.parse("67"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,JqValues.parse("67"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();
        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(aNode,rootNode,abValue01,null,null,-1,-1,true);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(aValue01),found.toString());
        assertTrue(found.contains(aValue02),found.toString());

    }

    @Test
    public void findMatchingFingerprint_cousin() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity aaNode = new JqNode("aa");
        aaNode.sources=List.of(aNode);
        aaNode.persist();
        NodeEntity bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();
        NodeEntity bbNode = new JqNode("bb");
        bbNode.sources=List.of(bNode);
        bbNode.persist();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqString.of("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqString.of("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqString.of("a1"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,JqString.of("a2"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity aaValue01 = new ValueEntity(null,aaNode,JqString.of("fp"));
        aaValue01.sources=List.of(aValue01);
        aaValue01.persist();
        ValueEntity aaValue02 = new ValueEntity(null,aaNode,JqString.of("fp"));
        aaValue02.sources=List.of(aValue02);
        aaValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,JqString.of("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,JqString.of("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();

        ValueEntity bbValue01 = new ValueEntity(null,bbNode,JqString.of("bb1"));
        bbValue01.sources=List.of(bValue01);
        bbValue01.persist();
        ValueEntity bbValue02 = new ValueEntity(null,bbNode,JqString.of("bb2"));
        bbValue02.sources=List.of(bValue02);
        bbValue02.persist();
        tm.commit();

        List<ValueEntity> found = valueService.findMatchingFingerprint(bbNode,rootNode,aaValue01,null,null,-1,-1,true);
        assertTrue(found.contains(bbValue01),"found should contain bbValue01: "+found.toString());
        assertTrue(found.contains(bbValue02),"found should contain bbValue02: "+found.toString());

    }

    @Test
    public void findMatchingFingerprint() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();

        NodeGroupEntity group = new NodeGroupEntity("findMatchingFingerprint");
        group.persist();
        NodeEntity rootNode = group.root;
        NodeEntity aNode = new JqNode("a");
        aNode.group = group;
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b");
        bNode.group = group;
        bNode.sources=List.of(rootNode);
        bNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqString.of("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqString.of("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqString.of("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,JqString.of("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,JqString.of("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,JqString.of("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();
        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(rootNode,rootNode,aValue01,bNode);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(rootValue01),found.toString());
        assertTrue(found.contains(rootValue02),found.toString());

        assertEquals(rootValue01,found.get(0),found.toString());
        assertEquals(rootValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprint_sibling() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist(rootNode);
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist(aNode);
        NodeEntity bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();
        NodeEntity cNode = new JqNode("c");
        cNode.sources=List.of(rootNode);
        cNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqString.of("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqString.of("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,JqString.of("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,JqString.of("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,JqString.of("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,JqString.of("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();


        ValueEntity cValue01 = new ValueEntity(null,cNode,JqString.of("c1"));
        cValue01.sources=List.of(rootValue01);
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,JqString.of("c2"));
        cValue02.sources=List.of(rootValue02);
        cValue02.persist();


        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(cNode,rootNode,aValue01,bNode);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(cValue01),found.toString());
        assertTrue(found.contains(cValue02),found.toString());

        assertEquals(cValue01,found.get(0),found.toString());
        assertEquals(cValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprint_sorting_cousin_reverse() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a",rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b",rootNode);
        bNode.persist();
        NodeEntity cNode = new JqNode("c",".c",rootNode);
        cNode.persist();
        NodeEntity caNode = new JqNode("c",".ca",List.of(cNode,aNode));
        caNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,
            JqValues.parse("""
            { "a" : "a", "b" : "b1", "c" : "c1", "ca" : "ca1" }
            """));
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,
                JqValues.parse("""
            { "a" : "a", "b" : "b2", "c" : "c1", "ca" : "ca2" }
            """));
        rootValue01.persist();
        // a values
        ValueEntity aValue01 = new ValueEntity(null,aNode,rootValue01.data.getField("a"),List.of(rootValue01));
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,rootValue02.data.getField("a"),List.of(rootValue02));
        aValue02.persist();
        // b values
        ValueEntity bValue01 = new ValueEntity(null,bNode,rootValue01.data.getField("b"),List.of(rootValue01));
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,rootValue02.data.getField("b"),List.of(rootValue02));
        bValue02.persist();
        // c values
        ValueEntity cValue01 = new ValueEntity(null,cNode,rootValue01.data.getField("c"),List.of(rootValue01));
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,rootValue02.data.getField("c"),List.of(rootValue02));
        cValue02.persist();
        // ca values
        ValueEntity caValue01 = new ValueEntity(null,caNode,rootValue01.data.getField("ca"),List.of(rootValue01));
        caValue01.persist();
        ValueEntity caValue02 = new ValueEntity(null,caNode,rootValue02.data.getField("ca"),List.of(rootValue02));
        caValue02.persist();

        tm.commit();

        List<ValueEntity> found = valueService.findMatchingFingerprint(caNode,rootNode,aValue01,bNode,null,-1,-1,false);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(caValue01),found.toString());
        assertTrue(found.contains(caValue02),found.toString());

        assertEquals(caValue01,found.get(0),found.toString());
        assertEquals(caValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprint_sorting_cousin() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a",rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b",rootNode);
        bNode.persist();
        NodeEntity cNode = new JqNode("c",".c",rootNode);
        cNode.persist();
        NodeEntity caNode = new JqNode("c",".ca",List.of(cNode,aNode));
        caNode.persist();


        ValueEntity rootValue01 = new ValueEntity(null,rootNode,
                JqValues.parse("""
            { "a" : "a", "b" : "b1", "c" : "c1", "ca" : "ca1" }
            """));
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,
                JqValues.parse("""
            { "a" : "a", "b" : "b2", "c" : "c1", "ca" : "ca2" }
            """));
        rootValue01.persist();
        // a values
        ValueEntity aValue01 = new ValueEntity(null,aNode,rootValue01.data.getField("a"),List.of(rootValue01));
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,rootValue02.data.getField("a"),List.of(rootValue02));
        aValue02.persist();
        // b values
        ValueEntity bValue01 = new ValueEntity(null,bNode,rootValue01.data.getField("b"),List.of(rootValue01));
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,rootValue02.data.getField("b"),List.of(rootValue02));
        bValue02.persist();
        // c values
        ValueEntity cValue01 = new ValueEntity(null,cNode,rootValue01.data.getField("c"),List.of(rootValue01));
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,rootValue02.data.getField("c"),List.of(rootValue02));
        cValue02.persist();
        // ca values
        ValueEntity caValue01 = new ValueEntity(null,caNode,rootValue01.data.getField("ca"),List.of(rootValue01));
        caValue01.persist();
        ValueEntity caValue02 = new ValueEntity(null,caNode,rootValue02.data.getField("ca"),List.of(rootValue02));
        caValue02.persist();

        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(caNode,rootNode,aValue01,bNode);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(caValue01),found.toString());
        assertTrue(found.contains(caValue02),found.toString());

        assertEquals(caValue01,found.get(0),found.toString());
        assertEquals(caValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprint_dataset_sibling() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity("findMatchingFingeprint_dataset_siblilng");
        group.persist();
        NodeEntity rootNode = group.root;
        NodeEntity aNode = new JqNode("a",".a[]",List.of(rootNode));
        //aNode.group=group;
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b",List.of(aNode));
        //bNode.group=group;
        bNode.persist();
        NodeEntity cNode = new JqNode("c",".c",List.of(aNode));
        //cNode.group=group;
        cNode.persist();
        NodeEntity dNode = new JqNode("d",".d",List.of(aNode));
        //dNode.group=group;
        dNode.persist();


        ValueEntity rootValue = new ValueEntity(null,rootNode,JqValues.parse("""
                { "a": [ { "b" : 1, "c" : "cat", "d" : 1}, { "b" : 2, "c" : "cat", "d" : 2 } ] }
                """));
        rootValue.persist();
        ValueEntity a1 = new ValueEntity(null,aNode,rootValue.data.getField("a").getElement(0),List.of(rootValue));
        a1.persist();
        ValueEntity a2 = new ValueEntity(null,aNode,rootValue.data.getField("a").getElement(1),List.of(rootValue));
        a2.persist();

        ValueEntity b1 = new ValueEntity(null,bNode,a1.data.getField("b"),List.of(a1));
        b1.persist();
        ValueEntity b2 = new ValueEntity(null,bNode,a2.data.getField("b"),List.of(a2));
        b2.persist();

        ValueEntity c1 = new ValueEntity(null,cNode,a1.data.getField("c"),List.of(a1));
        c1.persist();
        ValueEntity c2 = new ValueEntity(null,cNode,a2.data.getField("c"),List.of(a2));
        c2.persist();

        ValueEntity d1 = new ValueEntity(null,cNode,a1.data.getField("d"),List.of(a1));
        d1.persist();
        ValueEntity d2 = new ValueEntity(null,cNode,a2.data.getField("d"),List.of(a2));
        d2.persist();
        tm.commit();

        List<ValueEntity> found = valueService.findMatchingFingerprint(
                bNode,
                aNode,
                c1,
                dNode,
                d1,
                null,
                2,
                0,
                false
        );

        assertTrue(found.contains(b1),"missing b1");
        assertTrue(found.contains(b2),"missing b2");
    }
    @Test
    public void findMatchingFingerprint_dataset_cousin() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity("findMatchingFingerprint_dataset_cousin");
        group.persist();
        NodeEntity rootNode = group.root;
        NodeEntity aNode = new JqNode("a",".a[]",List.of(rootNode));
        aNode.group=group;
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b",List.of(aNode));
        bNode.group=group;
        bNode.persist();
        NodeEntity cNode = new JqNode("c",".c",List.of(aNode));
        cNode.group=group;
        cNode.persist();
        NodeEntity baNode = new JqNode("ba",".ba",List.of(bNode));
        baNode.group=group;
        baNode.persist();
        NodeEntity caNode = new JqNode("ca",".ca",List.of(cNode));
        caNode.group=group;
        caNode.persist();



        ValueEntity rootValue01 = new ValueEntity(null,rootNode,JqString.of("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,JqString.of("root2"));
        rootValue02.persist();

        ValueEntity aValue011 = new ValueEntity(null,aNode,JqString.of("a11"));
        aValue011.sources=List.of(rootValue01);
        aValue011.persist();
        ValueEntity aValue012 = new ValueEntity(null,aNode,JqString.of("a12"));
        aValue012.sources=List.of(rootValue01);
        aValue012.persist();

        ValueEntity aValue021 = new ValueEntity(null,aNode,JqString.of("a21"));
        aValue021.sources=List.of(rootValue02);
        aValue021.persist();
        ValueEntity aValue022 = new ValueEntity(null,aNode,JqString.of("a22"));
        aValue022.sources=List.of(rootValue02);
        aValue022.persist();

        ValueEntity bValue011 = new ValueEntity(null,bNode,JqString.of("b11"));
        bValue011.sources=List.of(aValue011);
        bValue011.persist();
        ValueEntity bValue012 = new ValueEntity(null,bNode,JqString.of("b12"));
        bValue012.sources=List.of(aValue012);
        bValue012.persist();

        ValueEntity bValue021 = new ValueEntity(null,bNode,JqString.of("b21"));
        bValue021.sources=List.of(aValue021);
        bValue021.persist();
        ValueEntity bValue022 = new ValueEntity(null,bNode,JqString.of("b22"));
        bValue022.sources=List.of(aValue022);
        bValue022.persist();

        ValueEntity baValue011 = new ValueEntity(null,baNode,JqString.of("ba11"));
        baValue011.sources=List.of(bValue011);
        baValue011.persist();
        ValueEntity baValue012 = new ValueEntity(null,baNode,JqString.of("ba12"));
        baValue012.sources=List.of(bValue012);
        baValue012.persist();

        ValueEntity baValue021 = new ValueEntity(null,baNode,JqString.of("ba21"));
        baValue021.sources=List.of(bValue021);
        baValue021.persist();
        ValueEntity baValue022 = new ValueEntity(null,baNode,JqString.of("ba22"));
        baValue022.sources=List.of(bValue022);
        baValue022.persist();

        ValueEntity cValue011 = new ValueEntity(null,cNode,JqString.of("c11"));
        cValue011.sources=List.of(aValue011);
        cValue011.persist();
        ValueEntity cValue012 = new ValueEntity(null,cNode,JqString.of("c12"));
        cValue012.sources=List.of(aValue012);
        cValue012.persist();

        ValueEntity cValue021 = new ValueEntity(null,cNode,JqString.of("c21"));
        cValue021.sources=List.of(aValue021);
        cValue021.persist();
        ValueEntity cValue022 = new ValueEntity(null,cNode,JqString.of("c22"));
        cValue022.sources=List.of(aValue022);
        cValue022.persist();

        ValueEntity caValue011 = new ValueEntity(null,caNode,JqString.of("ca11"));
        caValue011.sources=List.of(cValue011);
        caValue011.persist();
        ValueEntity caValue012 = new ValueEntity(null,caNode,JqString.of("fp"));
        caValue012.sources=List.of(cValue012);
        caValue012.persist();

        ValueEntity caValue021 = new ValueEntity(null,caNode,JqString.of("ca21"));
        caValue021.sources=List.of(cValue021);
        caValue021.persist();
        ValueEntity caValue022 = new ValueEntity(null,caNode,JqString.of("fp"));
        caValue022.sources=List.of(cValue022);
        caValue022.persist();

        tm.commit();


        List<ValueEntity> found = valueService.findMatchingFingerprint(caNode,aNode,caValue022,baNode);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(caValue012),found.toString());
        assertTrue(found.contains(caValue022),found.toString());

        assertEquals(caValue012,found.get(0),found.toString());
        assertEquals(caValue022,found.get(1),found.toString());

    }

    //why did we need values to be sorted? They get sorted by node when calculating
    @Test @Disabled
    public void persist_fixes_source_order() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity a = new JqNode("a");
        a.sources=List.of(root);
        NodeEntity ab = new JqNode("ab");
        ab.sources=List.of(a);
        ab.persist();
        NodeEntity ac = new JqNode("ac");
        ac.sources=List.of(a);
        NodeEntity abc = new JqNode("abc");
        abc.sources=List.of(ab,ac,a,root);
        abc.persist();

        ValueEntity vr = new ValueEntity();
        vr.persist();
        ValueEntity va = new ValueEntity();
        va.sources=List.of(vr);
        va.persist();
        ValueEntity vab = new ValueEntity();
        vab.sources=List.of(va);
        vab.persist();
        ValueEntity vac = new ValueEntity();
        vac.sources=List.of(va);
        vac.persist();
        ValueEntity vabc =  new ValueEntity();
        vabc.sources=List.of(vab,vac,va,vr);
        vabc.persist();
        tm.commit();

        tm.begin();
        try{
            ValueEntity found = valueService.byId(vabc.id);
            List<ValueEntity> sources = found.sources;
            assertNotNull(sources);
            assertEquals(4, sources.size());

            assertTrue(sources.indexOf(va) < sources.indexOf(vab),"va should come before vab");
            assertTrue(sources.indexOf(va) < sources.indexOf(vac),"va should come before vac");
            assertTrue(sources.indexOf(vr) < sources.indexOf(va),"vr should come before va");
        }finally {
            tm.commit();
        }

    }

    @Test
    public void getValues() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        NodeEntity bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        ValueEntity rootValue = new ValueEntity();
        rootValue.node = root;
        rootValue.persist();

        ValueEntity alphaValue = new ValueEntity();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        ValueEntity bravoValue = new ValueEntity();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        ValueEntity bravobravoValue = new ValueEntity();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();
        tm.commit();

        List<ValueEntity> values = valueService.getValues(root);
        assertNotNull(values,"result should not be null");
        assertEquals(1,values.size(),"result should contain 1 value: "+values);
        assertTrue(values.contains(rootValue),"result should contain root value: "+values);

    }
    @Test
    public void getDescendantValues_value_node() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        NodeEntity bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        ValueEntity rootValue = new ValueEntity();
        rootValue.node = root;
        rootValue.persist();

        ValueEntity alphaValue = new ValueEntity();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        ValueEntity bravoValue = new ValueEntity();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        ValueEntity bravobravoValue = new ValueEntity();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<ValueEntity> found = valueService.getDescendantValues(rootValue,bravobravo);

        assertFalse(found.contains(rootValue),"descendants should not include self: "+found);
        assertTrue(found.contains(bravobravoValue),"descendants should include value from target node: "+found);
    }

    @Test
    public void getDescendantValues_excludes_root_when_same_node() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        // Single node — all values belong to the same node type
        NodeEntity node = new JqNode("sametype");
        node.persist();

        // root → child → grandchild, all on the same node
        ValueEntity rootValue = new ValueEntity(null, node, JqString.of("root-data"));
        rootValue.persist();

        ValueEntity childValue = new ValueEntity(null, node, JqString.of("child-data"));
        childValue.sources = List.of(rootValue);
        childValue.persist();

        ValueEntity grandchildValue = new ValueEntity(null, node, JqString.of("grandchild-data"));
        grandchildValue.sources = List.of(childValue);
        grandchildValue.persist();
        tm.commit();

        // Query descendants of rootValue filtered to the SAME node type
        List<ValueEntity> found = valueService.getDescendantValues(rootValue, node);

        assertFalse(found.contains(rootValue),
            "root value should not appear in its own descendants even when node types match");
        assertTrue(found.contains(childValue),
            "child value should be in descendants");
        assertTrue(found.contains(grandchildValue),
            "grandchild value should be in descendants");
        assertEquals(2, found.size(),
            "should find exactly 2 descendants: " + found);
    }

    @Test
    public void getDescendantValuesByNodes_excludes_root_when_same_node() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity node = new JqNode("sametype");
        node.persist();

        ValueEntity rootValue = new ValueEntity(null, node, JqString.of("root-data"));
        rootValue.persist();

        ValueEntity childValue = new ValueEntity(null, node, JqString.of("child-data"));
        childValue.sources = List.of(rootValue);
        childValue.persist();

        ValueEntity grandchildValue = new ValueEntity(null, node, JqString.of("grandchild-data"));
        grandchildValue.sources = List.of(childValue);
        grandchildValue.persist();
        tm.commit();

        Map<Long, List<ValueEntity>> found = valueService.getDescendantValuesByNodes(rootValue, List.of(node));

        List<ValueEntity> values = found.getOrDefault(node.getId(), List.of());
        assertFalse(values.contains(rootValue),
            "root value should not appear in its own descendants even when node types match");
        assertTrue(values.contains(childValue),
            "child value should be in descendants");
        assertTrue(values.contains(grandchildValue),
            "grandchild value should be in descendants");
        assertEquals(2, values.size(),
            "should find exactly 2 descendants: " + values);
    }

    @Test
    public void getDescendantValues_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        NodeEntity bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        ValueEntity rootValue = new ValueEntity();
        rootValue.node = root;
        rootValue.persist();

        ValueEntity alphaValue = new ValueEntity();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        ValueEntity bravoValue = new ValueEntity();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        ValueEntity bravobravoValue = new ValueEntity();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<Value> found = valueService.getNodeDescendantValues(root.id);

        assertEquals(3,found.size(),"expect to see three entries");
        assertTrue(found.contains(apiMapper.toValue(bravobravoValue, new CycleAvoidingContext())), "missing bravobravo[" + bravobravoValue.id + "]'s value: " + found);
    }

    @Test
    public void getDescendantValues_two_generations() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        NodeEntity bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(root,bravo);
        bravobravo.persist();

        ValueEntity rootValue = new ValueEntity();
        rootValue.node = root;
        rootValue.persist();

        ValueEntity alphaValue = new ValueEntity();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        ValueEntity bravoValue = new ValueEntity();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        ValueEntity bravobravoValue = new ValueEntity();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<ValueEntity> found = valueService.getDescendantValues(rootValue);
        assertEquals(3,found.size(),"expect to see three entries");
        assertTrue(found.contains(bravobravoValue),"missing bravobravo["+bravobravoValue.id+"]'s value: "+found);
        assertFalse(found.contains(rootValue),"root value should not be in results: "+found);
    }
    @Test
    public void getDirectDescendantValues() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity root = new JqNode("root");
        root.persist();
        NodeEntity alpha = new JqNode("alpha");
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo");
        bravo.persist();
        NodeEntity bravobravo = new JqNode("bravobravo");
        bravobravo.persist();

        ValueEntity rootValue = new ValueEntity();
        rootValue.node = root;
        rootValue.persist();

        ValueEntity alphaValue = new ValueEntity();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        ValueEntity bravoValue1 = new ValueEntity();
        bravoValue1.node = bravo;
        bravoValue1.sources = List.of(rootValue);
        bravoValue1.persist();

        ValueEntity bravoValue2 = new ValueEntity();
        bravoValue2.node = bravo;
        bravoValue2.sources = List.of(rootValue);
        bravoValue2.persist();

        ValueEntity bravobravoValue = new ValueEntity();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue1);
        bravobravoValue.persist();

        tm.commit();

        List<ValueEntity> found = valueService.getDirectDescendantValues(rootValue,bravo);
        assertEquals(2,found.size(),"expect to see 2 entry: "+found);
        assertFalse(found.contains(bravobravoValue),"should not contain bravobravo["+bravobravoValue.id+"]'s value: "+found);
    }
    @Test
    public void getGroupedValues_fingerprinted() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException
    {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity group = new JqNode("group",".group",rootNode);
        group.persist();
        NodeEntity alpha = new JqNode("alpha",".a",group);
        alpha.persist();
        NodeEntity bravo = new JqNode("bravo",".b",group);
        bravo.persist();
        NodeEntity charlie = new JqNode("charlie",".c",group);
        charlie.persist();
        ValueEntity root1 = new ValueEntity(null,rootNode,JqValues.parse(
            """
            {
              "group": [
                { "a": 1, "b": 1, "c": "oneone"},
                { "a": 1, "b": 2, "c": "onetwo"}
              ]
            }
            """
        ));
        root1.persist();
        ValueEntity group1_0 = new ValueEntity(null,group,root1.data.getField("group").getElement(0));
        group1_0.persist();
        ValueEntity alpha1_0 = new ValueEntity(null,alpha,group1_0.data.getField("a"),List.of(group1_0));
        alpha1_0.persist();
        ValueEntity bravo1_0 = new ValueEntity(null,bravo,group1_0.data.getField("b"),List.of(group1_0));
        bravo1_0.persist();
        ValueEntity charlie1_0 = new ValueEntity(null,charlie,group1_0.data.getField("c"),List.of(group1_0));
        charlie1_0.persist();
        ValueEntity group1_1 = new  ValueEntity(null,group,root1.data.getField("group").getElement(1));
        group1_1.persist();
        ValueEntity alpha1_1 = new ValueEntity(null,alpha,group1_1.data.getField("a"),List.of(group1_1));
        alpha1_1.persist();
        ValueEntity bravo1_1 = new ValueEntity(null,bravo,group1_1.data.getField("b"),List.of(group1_1));
        bravo1_1.persist();
        ValueEntity charlie1_1 = new ValueEntity(null,charlie,group1_1.data.getField("c"),List.of(group1_1));
        charlie1_1.persist();

        ValueEntity root2 = new ValueEntity(null,rootNode,JqValues.parse(
                """
                {
                  "group": [
                    { "a": 2, "b": 1, "c": "twoone"},
                    { "a": 2, "b": 2, "c": "twotwo"}
                  ]
                }
                """
        ));
        root2.persist();
        ValueEntity group2_0 = new ValueEntity(null,group,root2.data.getField("group").getElement(0));
        group2_0.persist();
        ValueEntity alpha2_0 = new ValueEntity(null,alpha,group2_0.data.getField("a"),List.of(group2_0));
        alpha2_0.persist();
        ValueEntity bravo2_0 = new ValueEntity(null,bravo,group2_0.data.getField("b"),List.of(group2_0));
        bravo2_0.persist();
        ValueEntity charlie2_0 = new ValueEntity(null,charlie,group2_0.data.getField("c"),List.of(group2_0));
        charlie2_0.persist();
        ValueEntity group2_1 = new  ValueEntity(null,group,root2.data.getField("group").getElement(1));
        group2_1.persist();
        ValueEntity alpha2_1 = new ValueEntity(null,alpha,group2_1.data.getField("a"),List.of(group2_1));
        alpha2_1.persist();
        ValueEntity bravo2_1 = new ValueEntity(null,bravo,group2_1.data.getField("b"),List.of(group2_1));
        bravo2_1.persist();
        ValueEntity charlie2_1 = new ValueEntity(null,charlie,group2_1.data.getField("c"),List.of(group2_1));
        charlie2_1.persist();
        tm.commit();


        //fetch only b=2
        List<JqValue> found = valueService.getGroupedValues(group.id,null,Map.of(bravo.id,bravo1_1.data),alpha.id);
        assertEquals(2,found.size());
        for(int i=0; i<found.size(); i++){
            assertEquals(bravo1_1.data.toString(),found.get(i).getField(bravo.name).toString());
        }
        //b=2 a=1
        found = valueService.getGroupedValues(group.id,null,Map.of(bravo.id,bravo1_1.data,alpha.id,alpha1_0.data),alpha.id);
        assertEquals(1,found.size());
        for(int i=0; i<found.size(); i++){
            assertEquals(bravo1_1.data.toString(),found.get(i).getField(bravo.name).toString());
            assertEquals(alpha1_0.data.toString(),found.get(i).getField(alpha.name).toString());
        }

    }

    @Test
    public void getGroupedValues_filtered() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity throughputNode = new JqNode("throughput");
        throughputNode.sources = List.of(rootNode);
        throughputNode.persist();
        NodeEntity buildIdNode = new JqNode("build_id");
        buildIdNode.sources = List.of(rootNode);
        buildIdNode.persist();

        ValueEntity root1 = new ValueEntity(null, rootNode, JqString.of("r1"));
        root1.persist();
        ValueEntity t1 = new ValueEntity(null, throughputNode, JqString.of("100"));
        t1.sources = List.of(root1);
        t1.persist();
        ValueEntity b1 = new ValueEntity(null, buildIdNode, JqString.of("201"));
        b1.sources = List.of(root1);
        b1.persist();
        tm.commit();

        List<JqValue> results = valueService.getGroupedValues(rootNode.id, List.of(throughputNode.id));

        assertEquals(1, results.size(), "expect one row: " + results);
        assertTrue(results.get(0).has("throughput"), "row should have throughput: " + results.get(0));
        assertFalse(results.get(0).has("build_id"), "row should not have build_id when filtered out: " + results.get(0));
    }

    @Test
    public void getGroupedValues_sorted() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException
    {

        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity throughputNode = new JqNode("throughput");
        throughputNode.sources = List.of(rootNode);
        throughputNode.persist();
        NodeEntity buildIdNode = new JqNode("build_id");
        buildIdNode.sources = List.of(rootNode);
        buildIdNode.persist();

        ValueEntity root1 = new ValueEntity(null, rootNode, JqString.of("r1"));
        root1.persist();
        ValueEntity throughputv1 = new ValueEntity(null, throughputNode, JqString.of("100"));
        throughputv1.sources = List.of(root1);
        throughputv1.persist();
        ValueEntity buildv1 = new ValueEntity(null, buildIdNode, JqString.of("202"));
        buildv1.sources = List.of(root1);
        buildv1.persist();

        ValueEntity root2 = new ValueEntity(null, rootNode, JqString.of("r2"));
        root2.persist();
        ValueEntity throughputv2 = new ValueEntity(null, throughputNode, JqString.of("101"));
        throughputv2.sources = List.of(root2);
        throughputv2.persist();
        ValueEntity buildv2 = new ValueEntity(null, buildIdNode, JqString.of("201"));
        buildv2.sources = List.of(root2);
        buildv2.persist();

        ValueEntity root3 = new ValueEntity(null, rootNode, JqString.of("r3"));
        root3.persist();
        ValueEntity throughputv3 = new ValueEntity(null, throughputNode, JqString.of("102"));
        throughputv3.sources = List.of(root3);
        throughputv3.persist();
        ValueEntity buildv3 = new ValueEntity(null, buildIdNode, JqString.of("200"));
        buildv3.sources = List.of(root3);
        buildv3.persist();
        tm.commit();

        List<JqValue> results = valueService.getGroupedValues(
                rootNode.id,
                List.of(throughputNode.id, buildIdNode.id),
                null,
                buildIdNode.id
        );

        assertEquals(3, results.size(), "expect 3 rows: " + results);
        // Grouped values may return numeric values as strings depending on DB aggregation
        assertEquals("200", results.get(0).getField("build_id").asText(), "first row should be build_id 200: " + results);
        assertEquals("201", results.get(1).getField("build_id").asText(), "second row should be build_id 201: " + results);
        assertEquals("202", results.get(2).getField("build_id").asText(), "third row should be build_id 202: " + results);
    }

    @Test
    public void getGroupedValues_empty_filterNodeIds_returns_all_nodes() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException
    {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity throughputNode = new JqNode("throughput");
        throughputNode.sources = List.of(rootNode);
        throughputNode.persist();
        NodeEntity buildNode = new JqNode("build_id");
        buildNode.sources = List.of(rootNode);
        buildNode.persist();

        ValueEntity root1 = new ValueEntity(null, rootNode, JqString.of("r1"));
        root1.persist();
        ValueEntity throughputv1 = new ValueEntity(null, throughputNode, JqString.of("100"));
        throughputv1.sources = List.of(root1);
        throughputv1.persist();
        ValueEntity buildv1 = new ValueEntity(null, buildNode, JqString.of("101"));
        buildv1.sources = List.of(root1);
        buildv1.persist();

        ValueEntity root2 = new ValueEntity(null, rootNode, JqString.of("r2"));
        root2.persist();
        ValueEntity throughputv2 = new ValueEntity(null, throughputNode, JqString.of("200"));
        throughputv2.sources = List.of(root2);
        throughputv2.persist();
        ValueEntity buildv2 = new ValueEntity(null, buildNode, JqString.of("201"));
        buildv2.sources = List.of(root2);
        buildv2.persist();

        ValueEntity root3 = new ValueEntity(null, rootNode, JqString.of("r3"));
        root3.persist();
        ValueEntity throughputv3 = new ValueEntity(null, throughputNode, JqString.of("300"));
        throughputv3.sources = List.of(root3);
        throughputv3.persist();
        ValueEntity buildv3 = new ValueEntity(null, buildNode, JqString.of("301"));
        buildv3.sources = List.of(root3);
        buildv3.persist();
        tm.commit();

        List<JqValue> results = valueService.getGroupedValues(rootNode.id, List.of());

        assertEquals(3, results.size(), "expect 3 rows: " + results);
        assertTrue(results.get(0).has("throughput"), "row should have throughput: " + results.get(0));
        assertTrue(results.get(0).has("build_id"), "row should have build_id: " + results.get(0));
        assertTrue(results.get(1).has("throughput"), "row should have throughput: " + results.get(1));
        assertTrue(results.get(1).has("build_id"), "row should have build_id: " + results.get(1));
        assertTrue(results.get(2).has("throughput"), "row should have throughput: " + results.get(2));
        assertTrue(results.get(2).has("build_id"), "row should have build_id: " + results.get(2));


    }

    @Test
    public void getLabelValues_empty_filterNodeIds_returns_all_nodes() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException
    {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity();
        group.persist();
        FolderEntity folder = new FolderEntity();
        folder.name = "lv-null-test";
        folder.group = group;
        folder.persist();

        NodeEntity rootNode = group.root;

        NodeEntity throughputNode = new JqNode("throughput");
        throughputNode.sources = List.of(rootNode);
        throughputNode.persist();
        NodeEntity buildIdNode = new JqNode("build_id");
        buildIdNode.sources = List.of(rootNode);
        buildIdNode.persist();

        ValueEntity root1 = new ValueEntity(null, rootNode, JqString.of("r1"));
        root1.persist();
        ValueEntity t1 = new ValueEntity(null, throughputNode, JqString.of("100"));
        t1.sources = List.of(root1);
        t1.persist();
        ValueEntity b1 = new ValueEntity(null, buildIdNode, JqString.of("201"));
        b1.sources = List.of(root1);
        b1.persist();

        ValueEntity root2 = new ValueEntity(null, rootNode, JqString.of("r2"));
        root2.persist();
        ValueEntity t2 = new ValueEntity(null, throughputNode, JqString.of("200"));
        t2.sources = List.of(root2);
        t2.persist();
        ValueEntity b2 = new ValueEntity(null, buildIdNode, JqString.of("202"));
        b2.sources = List.of(root2);
        b2.persist();

        tm.commit();

        List<JqValue> results = valueService.getLabelValues(folder.id, null, null, null);

        assertEquals(2, results.size(), "expect two rows: " + results);
        assertTrue(results.get(0).has("throughput"), "row should have throughput: " + results.get(0));
        assertTrue(results.get(0).has("build_id"), "row should have build_id: " + results.get(0));
        assertTrue(results.get(1).has("throughput"), "row should have throughput: " + results.get(1));
        assertTrue(results.get(1).has("build_id"), "row should have build_id: " + results.get(1));

    }

}


