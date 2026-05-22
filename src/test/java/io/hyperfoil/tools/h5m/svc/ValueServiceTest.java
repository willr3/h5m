package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.Value;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.mapper.ApiMapper;
import io.hyperfoil.tools.h5m.entity.mapper.CycleAvoidingContext;
import io.hyperfoil.tools.h5m.entity.node.FingerprintNode;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.*;
import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("a"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,new TextNode("a"));
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
    public void dependsOn() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name="root";
        rootNode.persist();
        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("root"));
        rootValue.persist();
        JqNode first = new JqNode("first",".first",rootNode);
        first.persist();
        ValueEntity firstValue = new ValueEntity(null,first,new TextNode("first"));
        firstValue.sources=List.of(rootValue);
        firstValue.persist();
        JqNode second = new JqNode("second",".second",first);
        second.persist();
        ValueEntity secondValue = new ValueEntity(null,second,new TextNode("second"));
        secondValue.sources=List.of(firstValue);
        secondValue.persist();
        JqNode third = new JqNode("third",".third",second);
        third.persist();
        ValueEntity thirdValue = new ValueEntity(null,third,new TextNode("third"));
        thirdValue.sources=List.of(secondValue);
        thirdValue.persist();
        tm.commit();

        assertTrue(valueService.dependsOn(firstValue,firstValue),"a value should depend on itself");
        assertTrue(valueService.dependsOn(thirdValue,firstValue),"third should depend on first");
        assertTrue(valueService.dependsOn(secondValue,firstValue),"second should depend on first");
        assertFalse(valueService.dependsOn(firstValue,thirdValue),"first should not depend on third");

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

        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,new TextNode("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null,bNode,new TextNode("b"));
        bValue.sources=List.of(rootValue);
        bValue.persist();
        // X is shared: both aValue and bValue are parents
        ValueEntity xValue = new ValueEntity(null,aNode,new TextNode("x"));
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

        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,new TextNode("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        // X has only aValue as parent
        ValueEntity xValue = new ValueEntity(null,aNode,new TextNode("x"));
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

        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null,aNode,new TextNode("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null,bNode,new TextNode("b"));
        bValue.sources=List.of(rootValue);
        bValue.persist();
        // X is shared between aValue and bValue
        ValueEntity xValue = new ValueEntity(null,aNode,new TextNode("x"));
        xValue.sources=List.of(aValue, bValue);
        xValue.persist();
        // Y is exclusive to aValue
        ValueEntity yValue = new ValueEntity(null,aNode,new TextNode("y"));
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

        ValueEntity rootValue = new ValueEntity(null, rootNode, new TextNode("root"));
        rootValue.persist();
        ValueEntity aValue = new ValueEntity(null, aNode, new TextNode("a"));
        aValue.sources = List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null, bNode, new TextNode("b"));
        bValue.sources = List.of(rootValue);
        bValue.persist();
        // shared: child of both aValue and bValue
        ValueEntity sharedValue = new ValueEntity(null, aNode, new TextNode("shared"));
        sharedValue.sources = List.of(aValue, bValue);
        sharedValue.persist();
        // exclusive: child of aValue only
        ValueEntity exclusiveValue = new ValueEntity(null, aNode, new TextNode("exclusive"));
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

        ValueEntity rootValue = new ValueEntity(null, rootNode, new TextNode("root"));
        rootValue.persist();
        // aValue is the purge target
        ValueEntity aValue = new ValueEntity(null, aNode, new TextNode("a"));
        aValue.sources = List.of(rootValue);
        aValue.persist();
        ValueEntity bValue = new ValueEntity(null, bNode, new TextNode("b"));
        bValue.sources = List.of(rootValue);
        bValue.persist();
        // child only reachable via aValue
        ValueEntity childOfA = new ValueEntity(null, aNode, new TextNode("childOfA"));
        childOfA.sources = List.of(aValue);
        childOfA.persist();
        // shared child with external parent bValue
        ValueEntity sharedChild = new ValueEntity(null, aNode, new TextNode("shared"));
        sharedChild.sources = List.of(aValue, bValue);
        sharedChild.persist();
        tm.commit();


        int purged = valueService.purge(aValue);


        tm.begin();
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
    public void lazy_data() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, JsonProcessingException {
        tm.begin();
        ObjectMapper objectMapper = new ObjectMapper();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        ValueEntity rootValue01 = new ValueEntity(null,rootNode,objectMapper.readTree("{\"this\":{ \"is\":{\"silly\":\"yes\"}}}"));
        rootValue01.persist();
        tm.commit();

        rootValue01 = null;

        List<ValueEntity> found = valueService.getValues(rootNode);
        assertEquals(1, found.size());
        for(ValueEntity v : found){
            assertThrows(LazyInitializationException.class, () -> {
                assertNull(v.data); // Access the lazy-loaded attribute
            });
        }


    }

    @Test
    public void getAncestor_single_ancestor() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, JsonProcessingException {
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
        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,objectMapper.readTree("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,objectMapper.readTree("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,objectMapper.readTree("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,objectMapper.readTree("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,objectMapper.readTree("1"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,objectMapper.readTree("2"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();

        ValueEntity abcValue01 = new ValueEntity(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));//matching value to find fingerprint
        abcValue01.sources=List.of(abValue01);
        abcValue01.persist();
        ValueEntity abcValue02 = new ValueEntity(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));
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

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root01"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root02"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,new TextNode("a01"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,new TextNode("a02"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,new TextNode("b01"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,new TextNode("b02"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();

        List<JsonNode> jsons = valueService.getGroupedValues(rootNode.id);

        assertEquals(2,jsons.size(),"expect to find two entries: "+jsons);

        JsonNode entry = jsons.get(0);
        assertNotNull(entry);
        assertInstanceOf(ObjectNode.class,entry);
        ObjectNode obj =  (ObjectNode) entry;
        assertTrue(obj.has("a"),"first entry should have a field: "+obj.toString());
        assertTrue(obj.has("b"),"first entry should have b field: "+obj.toString());
        entry = jsons.get(1);
        assertNotNull(entry);
        assertInstanceOf(ObjectNode.class,entry);
        obj =  (ObjectNode) entry;
        assertTrue(obj.has("a"),"second entry should have a field: "+obj.toString());
        assertTrue(obj.has("b"),"second entry should have b field: "+obj.toString());

        tm.commit();
    }

    @Test
    public void findMatchingFingerprint_deep_ancestry_json_value() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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
        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,objectMapper.readTree("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,objectMapper.readTree("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,objectMapper.readTree("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,objectMapper.readTree("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,objectMapper.readTree("1"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,objectMapper.readTree("2"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();

        ValueEntity abcValue01 = new ValueEntity(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));//matching value to find fingerprint
        abcValue01.sources=List.of(abValue01);
        abcValue01.persist();
        ValueEntity abcValue02 = new ValueEntity(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));
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
    public void findMatchingFingerprint_integer_value() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        NodeEntity abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,objectMapper.readTree("111"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,objectMapper.readTree("222"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,objectMapper.readTree("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,objectMapper.readTree("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity abValue01 = new ValueEntity(null,abNode,objectMapper.readTree("67"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        ValueEntity abValue02 = new ValueEntity(null,abNode,objectMapper.readTree("67"));
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

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,new TextNode("a1"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,new TextNode("a2"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity aaValue01 = new ValueEntity(null,aaNode,new TextNode("fp"));
        aaValue01.sources=List.of(aValue01);
        aaValue01.persist();
        ValueEntity aaValue02 = new ValueEntity(null,aaNode,new TextNode("fp"));
        aaValue02.sources=List.of(aValue02);
        aaValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,new TextNode("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();

        ValueEntity bbValue01 = new ValueEntity(null,bbNode,new TextNode("bb1"));
        bbValue01.sources=List.of(bValue01);
        bbValue01.persist();
        ValueEntity bbValue02 = new ValueEntity(null,bbNode,new TextNode("bb2"));
        bbValue02.sources=List.of(bValue02);
        bbValue02.persist();
        tm.commit();

        List<ValueEntity> found = valueService.findMatchingFingerprint(bbNode,rootNode,aaValue01,null,null,-1,-1,true);
        assertTrue(found.contains(bbValue01),"found should contain bbValue01: "+found.toString());
        assertTrue(found.contains(bbValue02),"found should contain bbValue02: "+found.toString());

    }

    @Test
    public void findMatchingFingerprint() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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
        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,new TextNode("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,new TextNode("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,new TextNode("b2"));
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
    public void findMatchingFIngerprint_from_ancestor_value() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, JsonProcessingException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name="root";
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a",rootNode);
        aNode.persist();
        NodeEntity bNode = new JqNode("b",".b",rootNode);
        bNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();
        ValueEntity root1 = new ValueEntity(null,rootNode,objectMapper.readTree("{ \"a\": \"a\", \"b\": \"b1\"}"));
        root1.persist();
        ValueEntity a1 = new ValueEntity(null,aNode,root1.data.get("a"),List.of(root1));
        a1.persist();
        ValueEntity b1 = new ValueEntity(null,bNode,root1.data.get("b"),List.of(root1));
        b1.persist();
        ValueEntity root2 = new ValueEntity(null,rootNode,objectMapper.readTree("{ \"a\": \"a\", \"b\": \"b2\"}"));
        root2.persist();
        ValueEntity a2 = new ValueEntity(null,aNode,root2.data.get("a"),List.of(root2));
        a2.persist();
        ValueEntity b2 = new ValueEntity(null,bNode,root2.data.get("b"),List.of(root2));
        b2.persist();
        tm.commit();

        List<ValueEntity> found = valueService.findMatchingFingerprint(bNode,rootNode,a2,null,null,root2,-1,-1,true);

        assertEquals(1,found.size(),found.toString());

    }
    @Test
    public void findMatchingFingerprint_sibling() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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
        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        ValueEntity aValue01 = new ValueEntity(null,aNode,new TextNode("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        ValueEntity aValue02 = new ValueEntity(null,aNode,new TextNode("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        ValueEntity bValue01 = new ValueEntity(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,new TextNode("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();


        ValueEntity cValue01 = new ValueEntity(null,cNode,new TextNode("c1"));
        cValue01.sources=List.of(rootValue01);
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,new TextNode("c2"));
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
    public void findMatchingFingerprint_sorting_cousin_reverse() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, JsonProcessingException {
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

        ObjectMapper objectMapper = new ObjectMapper();
        ValueEntity rootValue01 = new ValueEntity(null,rootNode,
            objectMapper.readTree("""
            { "a" : "a", "b" : "b1", "c" : "c1", "ca" : "ca1" }
            """));
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,
                objectMapper.readTree("""
            { "a" : "a", "b" : "b2", "c" : "c1", "ca" : "ca2" }
            """));
        rootValue01.persist();
        // a values
        ValueEntity aValue01 = new ValueEntity(null,aNode,rootValue01.data.get("a"),List.of(rootValue01));
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,rootValue02.data.get("a"),List.of(rootValue02));
        aValue02.persist();
        // b values
        ValueEntity bValue01 = new ValueEntity(null,bNode,rootValue01.data.get("b"),List.of(rootValue01));
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,rootValue02.data.get("b"),List.of(rootValue02));
        bValue02.persist();
        // c values
        ValueEntity cValue01 = new ValueEntity(null,cNode,rootValue01.data.get("c"),List.of(rootValue01));
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,rootValue02.data.get("c"),List.of(rootValue02));
        cValue02.persist();
        // ca values
        ValueEntity caValue01 = new ValueEntity(null,caNode,rootValue01.data.get("ca"),List.of(rootValue01));
        caValue01.persist();
        ValueEntity caValue02 = new ValueEntity(null,caNode,rootValue02.data.get("ca"),List.of(rootValue02));
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
    public void findMatchingFingerprint_sorting_cousin() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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

        ObjectMapper objectMapper = new ObjectMapper();
        ValueEntity rootValue01 = new ValueEntity(null,rootNode,
                objectMapper.readTree("""
            { "a" : "a", "b" : "b1", "c" : "c1", "ca" : "ca1" }
            """));
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,
                objectMapper.readTree("""
            { "a" : "a", "b" : "b2", "c" : "c1", "ca" : "ca2" }
            """));
        rootValue01.persist();
        // a values
        ValueEntity aValue01 = new ValueEntity(null,aNode,rootValue01.data.get("a"),List.of(rootValue01));
        aValue01.persist();
        ValueEntity aValue02 = new ValueEntity(null,aNode,rootValue02.data.get("a"),List.of(rootValue02));
        aValue02.persist();
        // b values
        ValueEntity bValue01 = new ValueEntity(null,bNode,rootValue01.data.get("b"),List.of(rootValue01));
        bValue01.persist();
        ValueEntity bValue02 = new ValueEntity(null,bNode,rootValue02.data.get("b"),List.of(rootValue02));
        bValue02.persist();
        // c values
        ValueEntity cValue01 = new ValueEntity(null,cNode,rootValue01.data.get("c"),List.of(rootValue01));
        cValue01.persist();
        ValueEntity cValue02 = new ValueEntity(null,cNode,rootValue02.data.get("c"),List.of(rootValue02));
        cValue02.persist();
        // ca values
        ValueEntity caValue01 = new ValueEntity(null,caNode,rootValue01.data.get("ca"),List.of(rootValue01));
        caValue01.persist();
        ValueEntity caValue02 = new ValueEntity(null,caNode,rootValue02.data.get("ca"),List.of(rootValue02));
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
    public void findMatchingFingerprint_dataset_cousin() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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

        ObjectMapper objectMapper = new ObjectMapper();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        ValueEntity aValue011 = new ValueEntity(null,aNode,new TextNode("a11"));
        aValue011.sources=List.of(rootValue01);
        aValue011.persist();
        ValueEntity aValue012 = new ValueEntity(null,aNode,new TextNode("a12"));
        aValue012.sources=List.of(rootValue01);
        aValue012.persist();

        ValueEntity aValue021 = new ValueEntity(null,aNode,new TextNode("a21"));
        aValue021.sources=List.of(rootValue02);
        aValue021.persist();
        ValueEntity aValue022 = new ValueEntity(null,aNode,new TextNode("a22"));
        aValue022.sources=List.of(rootValue02);
        aValue022.persist();

        ValueEntity bValue011 = new ValueEntity(null,bNode,new TextNode("b11"));
        bValue011.sources=List.of(aValue011);
        bValue011.persist();
        ValueEntity bValue012 = new ValueEntity(null,bNode,new TextNode("b12"));
        bValue012.sources=List.of(aValue012);
        bValue012.persist();

        ValueEntity bValue021 = new ValueEntity(null,bNode,new TextNode("b21"));
        bValue021.sources=List.of(aValue021);
        bValue021.persist();
        ValueEntity bValue022 = new ValueEntity(null,bNode,new TextNode("b22"));
        bValue022.sources=List.of(aValue022);
        bValue022.persist();

        ValueEntity baValue011 = new ValueEntity(null,baNode,new TextNode("ba11"));
        baValue011.sources=List.of(bValue011);
        baValue011.persist();
        ValueEntity baValue012 = new ValueEntity(null,baNode,new TextNode("ba12"));
        baValue012.sources=List.of(bValue012);
        baValue012.persist();

        ValueEntity baValue021 = new ValueEntity(null,baNode,new TextNode("ba21"));
        baValue021.sources=List.of(bValue021);
        baValue021.persist();
        ValueEntity baValue022 = new ValueEntity(null,baNode,new TextNode("ba22"));
        baValue022.sources=List.of(bValue022);
        baValue022.persist();

        ValueEntity cValue011 = new ValueEntity(null,cNode,new TextNode("c11"));
        cValue011.sources=List.of(aValue011);
        cValue011.persist();
        ValueEntity cValue012 = new ValueEntity(null,cNode,new TextNode("c12"));
        cValue012.sources=List.of(aValue012);
        cValue012.persist();

        ValueEntity cValue021 = new ValueEntity(null,cNode,new TextNode("c21"));
        cValue021.sources=List.of(aValue021);
        cValue021.persist();
        ValueEntity cValue022 = new ValueEntity(null,cNode,new TextNode("c22"));
        cValue022.sources=List.of(aValue022);
        cValue022.persist();

        ValueEntity caValue011 = new ValueEntity(null,caNode,new TextNode("ca11"));
        caValue011.sources=List.of(cValue011);
        caValue011.persist();
        ValueEntity caValue012 = new ValueEntity(null,caNode,new TextNode("fp"));
        caValue012.sources=List.of(cValue012);
        caValue012.persist();

        ValueEntity caValue021 = new ValueEntity(null,caNode,new TextNode("ca21"));
        caValue021.sources=List.of(cValue021);
        caValue021.persist();
        ValueEntity caValue022 = new ValueEntity(null,caNode,new TextNode("fp"));
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

        ValueEntity vr = new ValueEntity(null,root);
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


        try{
            tm.begin();
            ValueEntity found = valueService.byId(vabc.id);
            List<ValueEntity> sources = found.sources;
            int size = sources.size();
            tm.commit();
            assertNotNull(sources);
            assertEquals(4, size);

            assertTrue(sources.indexOf(va) < sources.indexOf(vab),"va should come before vab");
            assertTrue(sources.indexOf(va) < sources.indexOf(vac),"va should come before vac");
            assertTrue(sources.indexOf(vr) < sources.indexOf(va),"vr should come before va");
        }finally {

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
        ValueEntity rootValue = new ValueEntity(null, node, new TextNode("root-data"));
        rootValue.persist();

        ValueEntity childValue = new ValueEntity(null, node, new TextNode("child-data"));
        childValue.sources = List.of(rootValue);
        childValue.persist();

        ValueEntity grandchildValue = new ValueEntity(null, node, new TextNode("grandchild-data"));
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

        ValueEntity rootValue = new ValueEntity(null, node, new TextNode("root-data"));
        rootValue.persist();

        ValueEntity childValue = new ValueEntity(null, node, new TextNode("child-data"));
        childValue.sources = List.of(rootValue);
        childValue.persist();

        ValueEntity grandchildValue = new ValueEntity(null, node, new TextNode("grandchild-data"));
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
}
