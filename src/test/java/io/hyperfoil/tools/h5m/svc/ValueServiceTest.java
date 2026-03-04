package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.JqNode;
import io.hyperfoil.tools.h5m.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ValueServiceTest extends FreshDb {

    @Inject
    ValueService valueService;

    @Inject
    TransactionManager tm;

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

        List<JsonNode> jsons = valueService.getGroupedValues(rootNode);

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
    public void findMatchingFingerprint_sorting_cousin() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
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
        NodeEntity caNode = new JqNode("ca");
        caNode.sources=List.of(cNode);
        caNode.persist();
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

        ValueEntity caValue01 = new ValueEntity(null,caNode,new TextNode("ca1"));
        caValue01.sources=List.of(cValue01);
        caValue01.persist();
        ValueEntity caValue02 = new ValueEntity(null,caNode,new TextNode("ca2"));
        caValue02.sources=List.of(cValue02);
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

        List<ValueEntity> found = valueService.getDescendantValues(root);

        assertEquals(3,found.size(),"expect to see three entries");
        assertTrue(found.contains(bravobravoValue),"missing bravobravo["+bravobravoValue.id+"]'s value: "+found);
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
