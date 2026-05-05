package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import static io.hyperfoil.tools.h5m.entity.NodeEntity.FQDN_SEPARATOR;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class NodeServiceTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    NodeService nodeService;
    @Inject
    ValueService valueService;

    @Test
    public void create_without_group() {
        NodeEntity jqNode = new JqNode("foo",".bar");
        NodeEntity response = nodeService.create(jqNode);
        assertNotNull(response.id);
        assertTrue(response.id > 0);
        assertEquals(jqNode.id, response.id);
    }
    @Test
    public void delete_without_group(){
        NodeEntity jqNode = new JqNode("foo",".bar");
        NodeEntity response = nodeService.create(jqNode);
        nodeService.delete(jqNode.id);
        List<Node> found = nodeService.findNodeByFqdn("foo");
        assertEquals(0, found.size());
    }

    @Test
    public void delete_does_not_cascade_and_delete_ancestor() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        tm.commit();

        tm.begin();
        aNode.delete();
        tm.commit();

        NodeEntity found = NodeEntity.findById(rootNode.id);
        assertNotNull(found);


    }

    @Test
    public void delete_does_not_cascade_to_shared_dependent() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity sourceA = new JqNode("sourceA",".a");
        sourceA.sources=List.of(rootNode);
        sourceA.persist();
        NodeEntity sourceB = new JqNode("sourceB",".b");
        sourceB.sources=List.of(rootNode);
        sourceB.persist();
        // sharedNode has both sourceA and sourceB as parents
        NodeEntity sharedNode = new JqNode("shared",".shared");
        sharedNode.sources=List.of(sourceA, sourceB);
        sharedNode.persist();
        tm.commit();

        tm.begin();
        nodeService.delete(sourceA.id);
        tm.commit();

        // sharedNode should still exist because sourceB is still a parent
        NodeEntity foundShared = NodeEntity.findById(sharedNode.id);
        assertNotNull(foundShared, "shared node should not be deleted when one source is removed");
        // sourceB should still exist
        NodeEntity foundB = NodeEntity.findById(sourceB.id);
        assertNotNull(foundB, "sourceB should still exist");
    }

    @Test
    public void renameParameters_spaced_parameters() {
        assertEquals("function foo( biz , buz ){}", nodeService.renameParameters("function foo( fiz , fuzz ){}", Map.of("fiz", "biz", "fuzz", "buz")));
    }
    @Test
    public void renameParameters_nested_parameters() {
        assertEquals("function foo({biz,buz}){}", nodeService.renameParameters("function foo({fiz,fuzz}){}", Map.of("fiz", "biz", "fuzz", "buz")));
    }
    @Test
    public void renameParameters_skip_method_call() {
        assertEquals("buz=>buz.foo()", nodeService.renameParameters("foo=>foo.foo()", Map.of("foo", "buz")));
    }
    @Test
    public void renameParameters_string_literal() {
        assertEquals("buz=>`{buz}`", nodeService.renameParameters("foo=>`{foo}`", Map.of("foo", "buz")));
    }
    @Test
    public void renameParameter_skip_object_key(){
        assertEquals("buzz=>({foo:buzz})",nodeService.renameParameters("foo=>({foo:foo})",Map.of("foo","buzz")));
    }
    @Test
    public void renameParameter_tertiary_refernece(){
        assertEquals("(_a,_b,_c)=> _a ? _b: _c",nodeService.renameParameters("(a,b,c)=> a ? b: c",Map.of("a","_a","b","_b","c","_c")));
    }

    @Test
    public void update_changes_javascript_argument_name() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity n = new JqNode("oldName", "operation");
        n.persist();
        NodeEntity js = new JsNode("jsNode", "(oldName)=>oldName");
        js.sources=List.of(n);
        js.persist();
        tm.commit();

        n.name = "newName";

        nodeService.update(n);
        NodeEntity found = NodeEntity.findById(js.id);
        assertEquals("(newName)=>newName", found.operation,"the change should update method");

    }



    @Test
    public void calculateJsValue_yield() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsNode jsNode = new JsNode("js","function* foo(){ yield 1; yield 'foo'; }");
        List<ValueEntity> result = nodeService.calculateJsValues(jsNode, Collections.EMPTY_MAP,0);

        assertEquals(2,result.size());
        ValueEntity value = result.get(0);
        assertNotNull(value);
        JsonNode data = value.data;
        assertNotNull(data);
        assertEquals(new LongNode(1),data);
        value = result.get(1);
        assertNotNull(value);
        data = value.data;
        assertNotNull(data);
        assertEquals(new TextNode("foo"),data);
        System.out.println(data);
    }


    @Test
    public void calculateJsValue_no_source() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsNode jsNode = new JsNode("js","()=>123");
        List<ValueEntity> result = nodeService.calculateJsValues(jsNode, Collections.EMPTY_MAP,0);

        assertEquals(1,result.size());
        ValueEntity value = result.get(0);
        assertNotNull(value);
        JsonNode data = value.data;
        assertNotNull(data);
        assertEquals("123",data.toString());
    }
    @Test
    public void calculateJsValue_arrow_without_parenthesis_text() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name="root";
        rootNode.persist();
        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("Bright"));
        rootValue.persist();
        JsNode jsNode = new JsNode("js","root=>'Hi, '+root");
        jsNode.persist();
        tm.commit();

        Map<String, ValueEntity> combined = Map.of("root",rootValue);
        List<ValueEntity> result = nodeService.calculateJsValues(jsNode, combined,0);

        assertNotNull(result);
        assertEquals(1,result.size());
        ValueEntity first = result.get(0);
        assertNotNull(first);
        JsonNode data = first.data;
        assertNotNull(data);
        assertEquals("Hi, Bright",data.asText());
    }

    @Test
    public void calculateJsValue_arrow_different_parameter_name() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name="root";
        rootNode.persist();
        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("Bright"));
        rootValue.persist();
        JsNode jsNode = new JsNode("js","arg=>'Hi, '+arg");
        jsNode.sources=List.of(rootNode);
        jsNode.persist();
        tm.commit();

        assertEquals(1,jsNode.sources.size(),"jsNode should have a source");

        Map<String, ValueEntity> combined = Map.of("root",rootValue);
        List<ValueEntity> result = nodeService.calculateJsValues(jsNode, combined,0);

        assertNotNull(result);
        assertEquals(1,result.size());
        ValueEntity first = result.get(0);
        assertNotNull(first);
        JsonNode data = first.data;
        assertNotNull(data);
        assertEquals("Hi, Bright",data.asText());
    }

    @Test
    public void calculateJsValue_arrow_multiple_source_nodes() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name="root";

        rootNode.persist();

        JsNode otherSource = new JsNode("other","arg=>arg");
        otherSource.persist();
        ValueEntity rootValue = new ValueEntity(null,rootNode,new TextNode("Bright"));
        rootValue.persist();
        JsNode jsNode = new JsNode("js","arg=>arg");
        jsNode.sources = List.of(rootNode,otherSource);
        jsNode.persist();
        tm.commit();

        Map<String, ValueEntity> combined = Map.of("root",rootValue);
        List<ValueEntity> result = nodeService.calculateJsValues(jsNode, combined,0);

        assertNotNull(result);
        assertEquals(1,result.size());
        ValueEntity first = result.get(0);
        assertNotNull(first);
        JsonNode data = first.data;
        assertNotNull(data);
        assertInstanceOf(ObjectNode.class,data,"data should be a json object but was :"+data.toString());
    }



    @Test
    public void calculateSqlJsonpathValues() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathNode node = new SqlJsonpathNode("sql","$.buz",List.of(rootNode));//should be a different type of node?
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.node=rootNode;
        v1.persist();

        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateSqlJsonpathValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(1,calculated.size());

    }
    @Test
    public void calculateSqlJsonpathValues_null() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathNode node = new SqlJsonpathNode("sql","$.miss",List.of(rootNode));//should be a different type of node?
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.node=rootNode;
        v1.persist();

        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateSqlJsonpathValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(0,calculated.size());

    }

    @Test
    public void calculateJqValues_array_miss() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        JqNode node = new JqNode("jqall","[.miss[]?]",List.of(rootNode));
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.node=rootNode;
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        // JQ [.miss[]?] produces an empty array [], which is not null so it gets returned
        assertEquals(1,calculated.size());
        assertTrue(calculated.getFirst().data.isArray());
        assertEquals(0,calculated.getFirst().data.size(),"empty array for missing path");
    }

    @Test
    public void calculateJqValues_array_match() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        JqNode node = new JqNode("jqall","[.foo[]?]",List.of(rootNode));
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "biz": "cat"
                }
                """);
        v1.node=rootNode;
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(1,calculated.size());
        assertTrue(calculated.getFirst().data.isArray());
        assertEquals(2,calculated.getFirst().data.size(),"should contain both foo elements");
    }

    @Test
    public void calculateSqlAllJsonpathValues_null() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathAllNode node = new SqlJsonpathAllNode("sqlall","$.miss",List.of(rootNode));
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.node=rootNode;
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateSqlAllJsonpathValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(0,calculated.size());
    }

    @Test
    public void calculateSqlAllJsonpathValues_match() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathAllNode node = new SqlJsonpathAllNode("sqlall","$.foo",List.of(rootNode));
        node.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "biz": "cat"
                }
                """);
        v1.node=rootNode;
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> calculated = nodeService.calculateSqlAllJsonpathValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(1,calculated.size());
        assertTrue(calculated.getFirst().data.isArray());
    }

    @Test
    public void calculateSqlJsonpathValues_partial_match() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathNode hitNode = new SqlJsonpathNode("hit","$.biz",List.of(rootNode));
        hitNode.persist();
        SqlJsonpathNode missNode = new SqlJsonpathNode("miss","$.nope",List.of(rootNode));
        missNode.persist();

        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                { "biz": "cat", "buz": "dog" }
                """);
        v1.node=rootNode;
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<ValueEntity> hit = nodeService.calculateSqlJsonpathValues(hitNode,sourceValueMap,0);
        List<ValueEntity> miss = nodeService.calculateSqlJsonpathValues(missNode,sourceValueMap,0);

        assertEquals(1, hit.size(), "matching path should produce a value");
        assertEquals("cat", hit.getFirst().data.asText());
        assertEquals(0, miss.size(), "non-matching path should produce no values");
    }

    @Test
    public void calculateRelativeDifference_root() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity rangeNode = new JqNode("range",".y",rootNode);
        rangeNode.persist();
        NodeEntity domainNode = new JqNode("domain",".domain",rootNode);
        domainNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint",".fingerprint",rootNode);
        fingerprintNode.persist();

        ValueEntity rootValue01 = new ValueEntity(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        ValueEntity rootValue02 = new ValueEntity(null,rootNode,new TextNode("root2"));
        rootValue02.persist();
        ValueEntity rootValue03 = new ValueEntity(null,rootNode,new TextNode("root3"));
        rootValue03.persist();

        ValueEntity rangeValue01 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(1));
        rangeValue01.sources=List.of(rootValue01);
        rangeValue01.persist();
        ValueEntity rangeValue02 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(2));
        rangeValue02.sources=List.of(rootValue02);
        rangeValue02.persist();
        ValueEntity rangeValue03 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(3));
        rangeValue03.sources=List.of(rootValue03);
        rangeValue03.persist();

        //somehow domain values are missing value_edge...
        //LongNode.valueOf breaks this???
        ValueEntity domainValue01 = new ValueEntity(null,domainNode,DoubleNode.valueOf(10));
        domainValue01.sources=List.of(rootValue01);
        domainValue01.persist();
        ValueEntity domainValue02 = new ValueEntity(null,domainNode,DoubleNode.valueOf(20));
        domainValue02.sources=List.of(rootValue02);
        domainValue02.persist();
        ValueEntity domainValue03 = new ValueEntity(null,domainNode, DoubleNode.valueOf(30));
        domainValue03.sources=List.of(rootValue03);
        domainValue03.persist();

        ValueEntity fingerprintValue01 = new ValueEntity(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue01.sources=List.of(rootValue01);
        fingerprintValue01.persist();
        ValueEntity fingerprintValue02 = new ValueEntity(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue02.sources=List.of(rootValue02);
        fingerprintValue02.persist();
        ValueEntity fingerprintValue03 = new ValueEntity(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue03.sources=List.of(rootValue03);
        fingerprintValue03.persist();
        tm.commit();

        RelativeDifference relDifference = new RelativeDifference();
        //relDifference.name="max.y";
        relDifference.setFilter("max");
        relDifference.setWindow(1);
        relDifference.setMinPrevious(1);
        relDifference.setNodes(fingerprintNode,rootNode,rangeNode,domainNode);

        List<ValueEntity> found = nodeService.calculateRelativeDifferenceValues(relDifference,rootValue01,0);
        assertNotNull(found);
        assertEquals(1,found.size());
        ValueEntity value = found.getFirst();
        System.out.println("found\n"+found);
        System.out.println("value\n"+value+"\n"+value.data);

    }

    @Test
    public void getDependentNodes() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        RootNode root = new RootNode();
        NodeEntity n1 = nodeService.create(new JqNode("n1","n1",root));
        NodeEntity n11 = nodeService.create(new JqNode("n11","n11",n1));
        NodeEntity n12 = nodeService.create(new JqNode("n12","n12",n1));
        NodeEntity n121 = nodeService.create(new JqNode("n121","n121",n12));
        tm.commit();

        List<NodeEntity> found = nodeService.getDependentNodes(n1);
        assertNotNull(found);
        assertEquals(2,found.size(),"should find two nodes: "+found);
        assertTrue(found.contains(n11),"should find node11 "+n11+" : "+found);
        assertTrue(found.contains(n12),"should find node12 "+n12+" : "+found);
    }

    @Test
    public void calculateJqValues_single_key_sourceValue() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode root = new RootNode();
        root.persist();
        NodeEntity upload = new JqNode();//should be a different type of node?
        upload.name="upload";
        upload.persist();
        ValueEntity v1 = new ValueEntity();
        v1.node = root;
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put("upload",v1);

        JqNode node = new JqNode("foo",".foo");
        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(1,calculated.size(),"expect to create a single value from key");
    }
    @Test
    public void calculateJqValues_single_key_iterating() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode root = new RootNode();
        root.persist();
        NodeEntity upload = new JqNode();//should be a different type of node?
        upload.name="upload";
        upload.persist();
        ValueEntity v1 = new ValueEntity();
        v1.node=root;
        v1.data= mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k" : "tres"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put("upload",v1);

        JqNode node = new JqNode("foo",".foo[]");
        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(2,calculated.size(),"expect to create a multiple values from an output file with multiple roots");

        String first = calculated.getFirst().data.toString();
        String second = calculated.getLast().data.toString();

        assertTrue(first.contains("one"),"first returned value should have first match from jq: "+first);
        assertTrue(second.contains("two"),"second returned value should have second match from jq: "+second);
    }

    @Test
    public void calculateJqValues_multiple_sourceValues() throws IOException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        RootNode root = new RootNode();
        root.persist();
        ValueEntity v1 = new ValueEntity();
        v1.node=root;
        v1.data=new TextNode("cat");
        v1.persist();
        ValueEntity v2 = new ValueEntity();
        v2.node=root;
        v2.data=new TextNode("dog");
        v2.persist();

        JqNode node = new JqNode("foo",".");
        node.persist();

        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put("v1",v1);
        sourceValueMap.put("v2",v2);

        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(1,calculated.size(),"expect to create a single value from two sources");
        String read = calculated.getFirst().data.toString();
        assertTrue(read.contains("cat"),"first file should be in result: "+read);
        assertTrue(read.contains("dog"),"second file should be in result: "+read);
        assertTrue(read.startsWith("["),"value should be an array: "+read);
        assertTrue(read.endsWith("]"),"value should be an array: "+read);
    }
    //key based merging using Jsq
    @Test
    public void calculatejsValues_dataset_merge() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity transform1 = new JqNode("transform1","$.config");
        transform1.persist();
        NodeEntity transform2 = new JqNode("transform2","$.foo");
        transform2.persist();
        NodeEntity transform3 = new JqNode("transform3","$.bar");
        transform3.persist();
        JsNode dataset = new JsNode("dataset", """
                function* (value){
                  const keys = Object.keys(value);
                  const values = Object.values(value);
                  const length = Math.max(...Object.values(value).map(v => Array.isArray(v) ? v.length : 1))
                  const rtrn = []
                  for (let i=0; i<length; i++){
                     let entry = {}
                     for(const key of keys){
                       let toAdd = Array.isArray(value[key]) ? value[key].length > i ? value[key][i] : false : value[key]
                       if(toAdd){
                         entry[key]=toAdd
                       }
                     }
                     console.log("entry",JSON.stringify(entry,null,2)   )
                     rtrn.push(entry)
                     yield entry;
                  }
                  //return rtrn
                }
                """);
        dataset.persist();
        ObjectMapper mapper = new ObjectMapper();
        ValueEntity upload = new ValueEntity(null,root,mapper.readTree("""
            { "config": { "alpha" : "apple"},
              "foo": [ { "key" : "fooOne" } , { "key": "fooTwo" } , { "key" : "fooThree" }],
              "bar": [ { "key" : "barOne" } , { "key": "barTwo" } ]
            }
        """));
        upload.persist();
        ValueEntity t1 = new ValueEntity(null,transform1,upload.data.get("config"));
        t1.persist();
        ValueEntity t2 = new ValueEntity(null,transform1,upload.data.get("foo"));
        t2.persist();
        ValueEntity t3 = new ValueEntity(null,transform1,upload.data.get("bar"));
        t3.persist();
        tm.commit();

        List<ValueEntity> values = nodeService.calculateJsValues(dataset,Map.of("transform1",t1,"transform2",t2,"transform3",t3),0);
        assertEquals(3,values.size(),"expect to create a single value from two sources");
        assertNotNull(values.get(0).data,"value[0] data should not be null");
        assertTrue(Stream.of("transform1","transform2","transform3").allMatch(values.get(0).data::hasNonNull),"missing expected key from values[0]");
        assertNotNull(values.get(1).data,"value[1] data should not be null");
        assertTrue(Stream.of("transform1","transform2","transform3").allMatch(values.get(1).data::hasNonNull),"missing expected key from values[1]");
        assertNotNull(values.get(2).data,"value[2] data should not be null");
        assertTrue(Stream.of("transform1","transform2").allMatch(values.get(2).data::hasNonNull),"missing expected key from values[2]");


    }

    @Test
    public void calculatejqValues_dataset_merge() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity root = new RootNode();
        root.persist();
        NodeEntity transform1 = new JqNode("transform1","$.config");
        transform1.persist();
        NodeEntity transform2 = new JqNode("transform2","$.foo");
        transform2.persist();
        NodeEntity transform3 = new JqNode("transform3","$.bar");
        transform3.persist();
        JqNode dataset = new JqNode("dataset", """
                . as $a | ([.[]|if type=="array" then length else 1 end]|max) as $m | [range($m)|. as $p | [$a[]|if type=="object" then . elif type=="array" and $p<length then .[$p] else empty end]]
                """);
        dataset.persist();
        ObjectMapper mapper = new ObjectMapper();
        ValueEntity upload = new ValueEntity(null,root,mapper.readTree("""
            { "config": { "alpha" : "apple"},
              "foo": [ { "key" : "fooOne" } , { "key": "fooTwo" } , { "key" : "fooThree" }],
              "bar": [ { "key" : "barOne" } , { "key": "barTwo" } ]
            }
        """));
        upload.persist();
        ValueEntity t1 = new ValueEntity(null,transform1,upload.data.get("config"));
        t1.persist();
        ValueEntity t2 = new ValueEntity(null,transform1,upload.data.get("foo"));
        t2.persist();
        ValueEntity t3 = new ValueEntity(null,transform1,upload.data.get("bar"));
        t3.persist();
        tm.commit();

        List<ValueEntity> values = nodeService.calculateJqValues(dataset,Map.of("transform1",t1,"transform2",t2,"transform3",t3),0);
        assertEquals(1,values.size(),"expect to create a single value from two sources");
        ValueEntity found = values.get(0);
        assertNotNull(found.data,"found data should not be null");
        System.out.println(found.data);

    }

    @Test
    public void calculateJqValues_multiple_source_order() throws IOException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeEntity node1 = new JqNode();
        node1.name="v1";
        node1.persist();
        ValueEntity v1 = new ValueEntity();
        v1.data=new TextNode("cat");
        v1.node = node1;
        v1.persist();
        NodeEntity node2 = new JqNode();
        node2.name="v2";
        node2.persist();
        ValueEntity v2 = new ValueEntity();
        v2.data=new TextNode("dog");
        v2.node = node2;
        v2.persist();
        tm.commit();

        Map<String, ValueEntity> sourceValueMap = new HashMap<>();
        sourceValueMap.put("v1",v1);
        sourceValueMap.put("v2",v2);

        JqNode node = new JqNode("foo",".");

        node.sources=List.of(node1,node2);

        List<ValueEntity> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(1,calculated.size(),"expect to create a single value from two sources");
        String read = calculated.getFirst().data.toString();
        assertTrue(read.contains("cat"),"first file should be in result: "+read);
        assertTrue(read.contains("dog"),"second file should be in result: "+read);
        assertTrue(read.startsWith("["),"value should be an array: "+read);
        assertTrue(read.endsWith("]"),"value should be an array: "+read);
    }
    @Test
    public void findNodeByFqdn_group_name() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity("group1");
        group.persist();
        NodeEntity node = new JqNode("node1");
        node.group = group;
        node.persist();
        tm.commit();

        List<Node> found = nodeService.findNodeByFqdn(node.getFqdn());
        assertEquals( 1,found.size());
    }
    @Test
    public void findNodeByFqdn_parent_and_child_name() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity("group1");
        group.persist();
        NodeEntity parent = new JqNode("parent");
        parent.group = group;
        parent.persist();

        NodeEntity decoy = new JqNode("child",".decoy");
        decoy.group = group;
        decoy.persist();

        NodeEntity child = new JqNode("child",".correct");
        child.group = group;
        child.sources=List.of(parent);
        child.persist();

        tm.commit();

        List<Node> found = nodeService.findNodeByFqdn(parent.name + FQDN_SEPARATOR + child.name);
        assertEquals( 1,found.size());
        Node foundNode = found.getFirst();
        assertNotNull(foundNode);
        assertEquals(".correct",foundNode.operation());
    }
    @Test
    public void findNodeByFqdn_group_orginal_group_name() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeGroupEntity group = new NodeGroupEntity("group1");
        group.persist();
        NodeGroupEntity group2 = new NodeGroupEntity("group2");
        group2.persist();
        NodeEntity node = new JqNode("node1");
        node.group = group;
        node.setOriginalGroup(group2);
        node.persist();
        tm.commit();

        List<Node> found = nodeService.findNodeByFqdn(node.getFqdn());
        assertEquals( 1,found.size());
    }


    @Test
    public void calculateValues_single_key() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity upload = new RootNode();
        upload.name="upload";
        upload.persist();

        ValueEntity v1 = new ValueEntity();
        v1.data= mapper.readTree("""
            {"foo":{"uno":"one","dos":"two"}}
            """);
        v1.node = upload;
        v1.data = new ObjectMapper().readTree(
            """
            {"foo":{"uno":"one","dos":"two"}}
            """
        );
        v1.persist();

        NodeEntity foo = new JqNode();
        foo.name="foo";
        foo.operation=".foo";
        foo.sources=List.of(upload);
        foo.persist();
        tm.commit();

        List<ValueEntity> calculated = nodeService.calculateValues(foo,List.of(v1));
        tm.begin();
        assertEquals(1,calculated.size(),"expected to calculate one value from foo:"+calculated);

        ValueEntity calculatedFoo = calculated.get(0);
        assertNotNull(calculatedFoo,"calculated value for foo should not be null");
        String content = calculatedFoo.data.toString();
        assertTrue(content.contains("uno"),"content missing first key:\n"+content);
        assertTrue(content.contains("dos"),"content missing second key:\n"+content);
        tm.commit();
    }

    @Test
    public void calculateValues_scalarType_by_length() throws SystemException, NotSupportedException, IOException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        File v1File = Files.createTempFile(new Exception().getStackTrace()[0].getMethodName()+".",".json").toFile();
        v1File.deleteOnExit();
        Files.write(v1File.toPath(),
            """
            {
              "foo": [ { "key": "one"}, { "key" : "two" } ],
              "bar": [ { "k": "uno" }, { "k": "dos"} ],
              "biz": "cat",
              "buz": "dog"
            }
            """.getBytes());
        NodeEntity upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree(            """
            {
              "foo": [ { "key": "one"}, { "key" : "two" } ],
              "bar": [ { "k": "uno" }, { "k": "dos"} ],
              "biz": "cat",
              "buz": "dog"
            }
            """);
        v1.node = upload;
        v1.persist();

        NodeEntity foo = new JqNode("foo",".foo[]",upload);
        foo.persist();

        NodeEntity biz = new JqNode("biz",".biz",upload);
        biz.scalarMethod= NodeEntity.ScalarVariableMethod.First;
        biz.persist();

        NodeEntity buz = new JqNode("buz",".buz",upload);
        buz.scalarMethod= NodeEntity.ScalarVariableMethod.All;
        buz.persist();

        NodeEntity combined = new JqNode("combined",".",foo,biz,buz);
        combined.persist();
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(biz,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(buz,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        tm.commit();
        List<ValueEntity> calculated = nodeService.calculateValues(combined,List.of(v1));

        assertEquals(2,calculated.size(),"expect 2 values due to foo");
        String first = calculated.getFirst().data.toString();
        String second = calculated.get(1).data.toString();

        assertTrue(first.contains("one"),"first value not found");
        assertFalse(first.contains("two"),"first value not found");
        assertTrue(second.contains("two"),"first value not found");
        assertFalse(second.contains("one"),"first value not found");

        assertTrue(first.contains("cat"),"first should have cat: "+first);
        assertFalse(second.contains("cat"),"second should not have cat: "+second);
        assertTrue(first.contains("dog"),"first should have dog: "+first);
        assertTrue(second.contains("dog"),"second should have dog: "+second);
    }

    @Test
    public void calculateValues_scalarType_NxN() throws SystemException, NotSupportedException, IOException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        ValueEntity v1 = new ValueEntity();
        v1.data=mapper.readTree(                """
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"} ],
                  "biz": "cat",
                  "buz": "dog"
                }
                """);
        v1.node = upload;
        v1.persist();

        NodeEntity foo = new JqNode("foo",".foo[]",upload);
        foo.persist();

        NodeEntity bar = new JqNode("bar",".bar[]",upload);
        bar.persist();

        NodeEntity biz = new JqNode("biz",".biz",upload);
        biz.scalarMethod= NodeEntity.ScalarVariableMethod.First;
        biz.persist();

        NodeEntity buz = new JqNode("buz",".buz",upload);
        buz.scalarMethod= NodeEntity.ScalarVariableMethod.All;
        buz.persist();

        NodeEntity combined = new JqNode("combined",".",foo,bar,biz,buz);
        combined.multiType= NodeEntity.MultiIterationType.NxN;
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(bar,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(biz,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(buz,List.of(v1)).forEach(ValueEntity.getEntityManager()::merge);
        tm.commit();
        List<ValueEntity> calculated = nodeService.calculateValues(combined,List.of(v1));

        assertEquals(4,calculated.size(),"expect 4 values due to foo");
        String first = calculated.getFirst().data.toString();
        String second = calculated.get(1).data.toString();
        String third = calculated.get(2).data.toString();
        String fourth = calculated.get(3).data.toString();

        //check the scalars
        assertTrue(first.contains("cat"),"first should have cat: "+first);
        assertFalse(second.contains("cat"),"second should not have cat: "+second);
        assertFalse(third.contains("cat"),"third should not have cat: "+second);
        assertFalse(fourth.contains("cat"),"fourth should not have cat: "+second);
        assertTrue(first.contains("dog"),"first should have dog: "+first);
        assertTrue(second.contains("dog"),"second should have dog: "+second);
        assertTrue(third.contains("dog"),"third should have dog: "+second);
        assertTrue(fourth.contains("dog"),"fourth should have dog: "+second);

        //check the multivalues
        assertTrue(Stream.of("one","uno","cat","dog").allMatch(first::contains),"missing expected key in first: "+first);
        assertTrue(Stream.of("one","dos","dog").allMatch(second::contains),"missing expected key in second: "+second);
        assertTrue(Stream.of("two","uno","dog").allMatch(third::contains),"missing expected key in third: "+third);
        assertTrue(Stream.of("two","dos","dog").allMatch(fourth::contains),"missing expected key in fourth: "+fourth);

    }

    @Test
    public void calculateValues_NxN_complicated() throws SystemException, NotSupportedException, IOException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        File v1File = Files.createTempFile(new Exception().getStackTrace()[0].getMethodName()+".", ".json").toFile();
        v1File.deleteOnExit();
        Files.write(v1File.toPath(),
                """
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k": "tres"} ],
                  "biz": [ { "j": "ant" }, { "j": "bee"}, { "j": "cat"} ]
                }
                """.getBytes());
        NodeEntity upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        ValueEntity v1 = new ValueEntity();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k": "tres"} ],
                  "biz": [ { "j": "ant" }, { "j": "bee"}, { "j": "cat"} ]
                }
                """);
        v1.node = upload;
        v1.persist();

        NodeEntity foo = new JqNode("foo", ".foo[]", upload);
        foo.persist();

        NodeEntity bar = new JqNode("bar", ".bar[]", upload);
        bar.persist();

        NodeEntity biz = new JqNode("biz", ".biz[]", upload);
        biz.scalarMethod = NodeEntity.ScalarVariableMethod.First;
        biz.persist();

        NodeEntity combined = new JqNode("combined", ".", foo, bar, biz);
        combined.multiType = NodeEntity.MultiIterationType.NxN;
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo, List.of(v1) ).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(bar, List.of(v1) ).forEach(ValueEntity.getEntityManager()::merge);
        nodeService.calculateValues(biz, List.of(v1) ).forEach(ValueEntity.getEntityManager()::merge);
        tm.commit();

        List<ValueEntity> calculated = nodeService.calculateValues(combined,List.of(v1));
        assertEquals(18,calculated.size());
        List<String> content = calculated.stream().map(n-> {
            return n.data.toString();
        }).toList();
        List.of("one","two").forEach(key->{
            assertEquals(9,content.stream().filter(v->v.contains(key)).count(),"unexpected number of value with "+key);
        });
        List.of("uno","dos","tres","ant","bee","cat").forEach(key->{
            assertEquals(6,content.stream().filter(v->v.contains(key)).count(),"unexpected number of value with "+key);
        });
    }

    @Test
    public void calculateFpValues_structured_json() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        JqNode platformNode = new JqNode("platform",".platform",rootNode);
        platformNode.persist();
        JqNode buildTypeNode = new JqNode("buildType",".buildType",rootNode);
        buildTypeNode.persist();
        tm.commit();

        FingerprintNode fpNode = new FingerprintNode("fp","fp",List.of(platformNode, buildTypeNode));

        ValueEntity platformValue = new ValueEntity(null, platformNode, new TextNode("x86"));
        ValueEntity buildTypeValue = new ValueEntity(null, buildTypeNode, new TextNode("release"));

        Map<String, ValueEntity> sourceValues = new HashMap<>();
        sourceValues.put("platform", platformValue);
        sourceValues.put("buildType", buildTypeValue);

        List<ValueEntity> result = nodeService.calculateFpValues(fpNode, sourceValues, 0);

        assertEquals(1, result.size(), "should produce exactly one fingerprint value");
        ValueEntity fpValue = result.getFirst();
        assertNotNull(fpValue.data);
        assertTrue(fpValue.data instanceof ObjectNode, "fingerprint data should be an ObjectNode, not a hash: " + fpValue.data.getClass());
        ObjectNode fpObject = (ObjectNode) fpValue.data;
        assertTrue(fpObject.has("platform"), "fingerprint should contain 'platform' key");
        assertTrue(fpObject.has("buildType"), "fingerprint should contain 'buildType' key");
        assertEquals("x86", fpObject.get("platform").asText());
        assertEquals("release", fpObject.get("buildType").asText());
    }

    @Test
    public void calculateFpValues_sorted_keys() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        // define sources in reverse alphabetical order: platform before buildType
        JqNode platformNode = new JqNode("platform",".platform",rootNode);
        platformNode.persist();
        JqNode buildTypeNode = new JqNode("buildType",".buildType",rootNode);
        buildTypeNode.persist();
        tm.commit();

        FingerprintNode fpNode = new FingerprintNode("fp","fp",List.of(platformNode, buildTypeNode));

        ValueEntity platformValue = new ValueEntity(null, platformNode, new TextNode("x86"));
        ValueEntity buildTypeValue = new ValueEntity(null, buildTypeNode, new TextNode("release"));

        Map<String, ValueEntity> sourceValues = new HashMap<>();
        sourceValues.put("platform", platformValue);
        sourceValues.put("buildType", buildTypeValue);

        List<ValueEntity> result = nodeService.calculateFpValues(fpNode, sourceValues, 0);

        assertEquals(1, result.size());
        ObjectNode fpObject = (ObjectNode) result.getFirst().data;
        // verify keys are sorted alphabetically: buildType before platform
        List<String> keys = new ArrayList<>();
        fpObject.fieldNames().forEachRemaining(keys::add);
        assertEquals("buildType", keys.get(0), "first key should be 'buildType' (alphabetical order)");
        assertEquals("platform", keys.get(1), "second key should be 'platform' (alphabetical order)");
    }

    @Test
    public void evaluateFingerprintFilter_matching() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode fingerprint = mapper.readTree("{\"platform\":\"x86\",\"buildType\":\"release\"}");
        boolean result = nodeService.evaluateFingerprintFilter("(fp) => fp.platform === \"x86\"", fingerprint);
        assertTrue(result, "filter should match when platform is x86");
    }

    @Test
    public void evaluateFingerprintFilter_non_matching() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode fingerprint = mapper.readTree("{\"platform\":\"x86\",\"buildType\":\"release\"}");
        boolean result = nodeService.evaluateFingerprintFilter("(fp) => fp.platform === \"arm\"", fingerprint);
        assertFalse(result, "filter should not match when platform is x86 but filter expects arm");
    }

    @Test
    public void evaluateFingerprintFilter_null_passes_all() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode fingerprint = mapper.readTree("{\"platform\":\"x86\",\"buildType\":\"release\"}");
        boolean result = nodeService.evaluateFingerprintFilter(null, fingerprint);
        assertTrue(result, "null filter should pass all fingerprints");
    }

    @Test
    public void evaluateFingerprintFilter_compound_filter() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode fingerprint = mapper.readTree("{\"platform\":\"x86\",\"buildType\":\"release\"}");
        boolean result = nodeService.evaluateFingerprintFilter("(fp) => fp.platform === \"x86\" && fp.buildType === \"release\"", fingerprint);
        assertTrue(result, "compound filter should match when both conditions are true");
    }

    @Test
    public void calculateFpValues_with_qvss_data() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        JsonNode qvssData = new ObjectMapper().readTree(getClass().getClassLoader().getResourceAsStream("qvss/15763.json"));

        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name = "upload";
        rootNode.persist();

        ValueEntity rootValue = new ValueEntity(null, rootNode, qvssData);
        rootValue.persist();

        JqNode quarkusVersionNode = new JqNode("QUARKUS_VERSION", ".config.QUARKUS_VERSION", rootNode);
        quarkusVersionNode.persist();
        JqNode javaVersionNode = new JqNode("JAVA_VERSION", ".config.JAVA_VERSION", rootNode);
        javaVersionNode.persist();
        tm.commit();

        // calculate jq values for both nodes
        Map<String, ValueEntity> sourceValues = Map.of("upload", rootValue);
        tm.begin();
        List<ValueEntity> qvValues = nodeService.calculateJqValues(quarkusVersionNode, sourceValues, 0).stream().map(ValueEntity.getEntityManager()::merge).toList();
        List<ValueEntity> jvValues = nodeService.calculateJqValues(javaVersionNode, sourceValues, 0).stream().map(ValueEntity.getEntityManager()::merge).toList();
        tm.commit();

        assertEquals(1, qvValues.size(), "should extract one QUARKUS_VERSION value");
        assertEquals(1, jvValues.size(), "should extract one JAVA_VERSION value");

        // build fingerprint from those extracted values
        FingerprintNode fpNode = new FingerprintNode("fp", "fp", List.of(javaVersionNode, quarkusVersionNode));

        Map<String, ValueEntity> fpSourceValues = new HashMap<>();
        fpSourceValues.put("QUARKUS_VERSION", qvValues.getFirst());
        fpSourceValues.put("JAVA_VERSION", jvValues.getFirst());

        List<ValueEntity> fpResult = nodeService.calculateFpValues(fpNode, fpSourceValues, 0);

        assertEquals(1, fpResult.size(), "should produce exactly one fingerprint value");
        ObjectNode fpObject = (ObjectNode) fpResult.getFirst().data;

        // verify sorted keys and real data values
        List<String> keys = new ArrayList<>();
        fpObject.fieldNames().forEachRemaining(keys::add);
        assertEquals("JAVA_VERSION", keys.get(0), "first key should be JAVA_VERSION (sorted)");
        assertEquals("QUARKUS_VERSION", keys.get(1), "second key should be QUARKUS_VERSION (sorted)");
        assertEquals("22.3.r17-grl", fpObject.get("JAVA_VERSION").asText());
        assertEquals("3.0.0.Alpha5", fpObject.get("QUARKUS_VERSION").asText());
    }

    @Test
    public void calculateRelativeDifference_with_fingerprint_filter() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity rangeNode = new JqNode("range",".y",rootNode);
        rangeNode.persist();
        NodeEntity domainNode = new JqNode("domain",".domain",rootNode);
        domainNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint",".fingerprint",rootNode);
        fingerprintNode.persist();

        // create 3 root values for x86 and 3 for arm
        ValueEntity rootX86_01 = new ValueEntity(null,rootNode,new TextNode("rootX86_1"));
        rootX86_01.persist();
        ValueEntity rootX86_02 = new ValueEntity(null,rootNode,new TextNode("rootX86_2"));
        rootX86_02.persist();
        ValueEntity rootX86_03 = new ValueEntity(null,rootNode,new TextNode("rootX86_3"));
        rootX86_03.persist();

        ValueEntity rootArm_01 = new ValueEntity(null,rootNode,new TextNode("rootArm_1"));
        rootArm_01.persist();
        ValueEntity rootArm_02 = new ValueEntity(null,rootNode,new TextNode("rootArm_2"));
        rootArm_02.persist();
        ValueEntity rootArm_03 = new ValueEntity(null,rootNode,new TextNode("rootArm_3"));
        rootArm_03.persist();

        // x86 range values
        ValueEntity rangeX86_01 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(1));
        rangeX86_01.sources=List.of(rootX86_01);
        rangeX86_01.persist();
        ValueEntity rangeX86_02 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(2));
        rangeX86_02.sources=List.of(rootX86_02);
        rangeX86_02.persist();
        ValueEntity rangeX86_03 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(3));
        rangeX86_03.sources=List.of(rootX86_03);
        rangeX86_03.persist();

        // arm range values
        ValueEntity rangeArm_01 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(10));
        rangeArm_01.sources=List.of(rootArm_01);
        rangeArm_01.persist();
        ValueEntity rangeArm_02 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(20));
        rangeArm_02.sources=List.of(rootArm_02);
        rangeArm_02.persist();
        ValueEntity rangeArm_03 = new ValueEntity(null,rangeNode, DoubleNode.valueOf(30));
        rangeArm_03.sources=List.of(rootArm_03);
        rangeArm_03.persist();

        // x86 domain values
        ValueEntity domainX86_01 = new ValueEntity(null,domainNode,DoubleNode.valueOf(10));
        domainX86_01.sources=List.of(rootX86_01);
        domainX86_01.persist();
        ValueEntity domainX86_02 = new ValueEntity(null,domainNode,DoubleNode.valueOf(20));
        domainX86_02.sources=List.of(rootX86_02);
        domainX86_02.persist();
        ValueEntity domainX86_03 = new ValueEntity(null,domainNode,DoubleNode.valueOf(30));
        domainX86_03.sources=List.of(rootX86_03);
        domainX86_03.persist();

        // arm domain values
        ValueEntity domainArm_01 = new ValueEntity(null,domainNode,DoubleNode.valueOf(100));
        domainArm_01.sources=List.of(rootArm_01);
        domainArm_01.persist();
        ValueEntity domainArm_02 = new ValueEntity(null,domainNode,DoubleNode.valueOf(200));
        domainArm_02.sources=List.of(rootArm_02);
        domainArm_02.persist();
        ValueEntity domainArm_03 = new ValueEntity(null,domainNode,DoubleNode.valueOf(300));
        domainArm_03.sources=List.of(rootArm_03);
        domainArm_03.persist();

        // x86 fingerprint values (structured ObjectNode)
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode x86Fp = mapper.createObjectNode();
        x86Fp.put("platform", "x86");
        ValueEntity fpX86_01 = new ValueEntity(null,fingerprintNode, x86Fp);
        fpX86_01.sources=List.of(rootX86_01);
        fpX86_01.persist();
        ValueEntity fpX86_02 = new ValueEntity(null,fingerprintNode, x86Fp);
        fpX86_02.sources=List.of(rootX86_02);
        fpX86_02.persist();
        ValueEntity fpX86_03 = new ValueEntity(null,fingerprintNode, x86Fp);
        fpX86_03.sources=List.of(rootX86_03);
        fpX86_03.persist();

        // arm fingerprint values (structured ObjectNode)
        ObjectNode armFp = mapper.createObjectNode();
        armFp.put("platform", "arm");
        ValueEntity fpArm_01 = new ValueEntity(null,fingerprintNode, armFp);
        fpArm_01.sources=List.of(rootArm_01);
        fpArm_01.persist();
        ValueEntity fpArm_02 = new ValueEntity(null,fingerprintNode, armFp);
        fpArm_02.sources=List.of(rootArm_02);
        fpArm_02.persist();
        ValueEntity fpArm_03 = new ValueEntity(null,fingerprintNode, armFp);
        fpArm_03.sources=List.of(rootArm_03);
        fpArm_03.persist();
        tm.commit();

        // set up RelativeDifference with fingerprintFilter that only matches x86
        RelativeDifference relDifference = new RelativeDifference();
        relDifference.setFilter("max");
        relDifference.setWindow(1);
        relDifference.setMinPrevious(1);
        relDifference.setFingerprintFilter("(fp) => fp.platform === \"x86\"");
        relDifference.setNodes(fingerprintNode,rootNode,rangeNode,domainNode);

        List<ValueEntity> found = nodeService.calculateRelativeDifferenceValues(relDifference,rootX86_01,0);
        assertNotNull(found);
        assertFalse(found.isEmpty(), "x86 filtered results should not be empty");

        // verify that all filtered results contain x86 domain values (10, 20, 30)
        Set<Double> x86DomainValues = Set.of(10.0, 20.0, 30.0);
        for (ValueEntity v : found) {
            assertNotNull(v.data, "change detection result should have data");
            assertTrue(v.data.has("domainvalue"), "result should contain domainvalue field");
            double domainValue = v.data.get("domainvalue").asDouble();
            assertTrue(x86DomainValues.contains(domainValue),
                "filtered result domainvalue " + domainValue + " should be an x86 value (10, 20, or 30)");
        }

        // run from an arm root WITHOUT filter to prove arm data produces results
        RelativeDifference relDiffNoFilter = new RelativeDifference();
        relDiffNoFilter.setFilter("max");
        relDiffNoFilter.setWindow(1);
        relDiffNoFilter.setMinPrevious(1);
        relDiffNoFilter.setNodes(fingerprintNode,rootNode,rangeNode,domainNode);

        List<ValueEntity> armNoFilter = nodeService.calculateRelativeDifferenceValues(relDiffNoFilter,rootArm_01,0);
        assertFalse(armNoFilter.isEmpty(), "arm unfiltered results should not be empty");
        // verify arm results contain arm domain values (100, 200, 300)
        Set<Double> armDomainValues = Set.of(100.0, 200.0, 300.0);
        for (ValueEntity v : armNoFilter) {
            double domainValue = v.data.get("domainvalue").asDouble();
            assertTrue(armDomainValues.contains(domainValue),
                "arm unfiltered domainvalue " + domainValue + " should be an arm value (100, 200, or 300)");
        }

        // run from an arm root WITH x86 filter — filter should exclude the arm fingerprint
        RelativeDifference relDiffArmWithX86Filter = new RelativeDifference();
        relDiffArmWithX86Filter.setFilter("max");
        relDiffArmWithX86Filter.setWindow(1);
        relDiffArmWithX86Filter.setMinPrevious(1);
        relDiffArmWithX86Filter.setFingerprintFilter("(fp) => fp.platform === \"x86\"");
        relDiffArmWithX86Filter.setNodes(fingerprintNode,rootNode,rangeNode,domainNode);

        List<ValueEntity> armWithX86Filter = nodeService.calculateRelativeDifferenceValues(relDiffArmWithX86Filter,rootArm_01,0);
        assertTrue(armWithX86Filter.isEmpty(),
            "arm root with x86 filter should produce no results but found " + armWithX86Filter.size());
    }

    @Test
    public void calculateFixedThreshold_basic_violations() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity rangeNode = new JqNode("range", ".y", rootNode);
        rangeNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint", ".fingerprint", rootNode);
        fingerprintNode.persist();

        // Create 3 root values with range values [5.0, 50.0, 150.0]
        ValueEntity root1 = new ValueEntity(null, rootNode, new TextNode("root1"));
        root1.persist();
        ValueEntity root2 = new ValueEntity(null, rootNode, new TextNode("root2"));
        root2.persist();
        ValueEntity root3 = new ValueEntity(null, rootNode, new TextNode("root3"));
        root3.persist();

        ValueEntity range1 = new ValueEntity(null, rangeNode, DoubleNode.valueOf(5.0));
        range1.sources = List.of(root1);
        range1.persist();
        ValueEntity range2 = new ValueEntity(null, rangeNode, DoubleNode.valueOf(50.0));
        range2.sources = List.of(root2);
        range2.persist();
        ValueEntity range3 = new ValueEntity(null, rangeNode, DoubleNode.valueOf(150.0));
        range3.sources = List.of(root3);
        range3.persist();

        // Fingerprint values
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode fpData = mapper.createObjectNode();
        fpData.put("env", "test");
        ValueEntity fp1 = new ValueEntity(null, fingerprintNode, fpData);
        fp1.sources = List.of(root1);
        fp1.persist();
        ValueEntity fp2 = new ValueEntity(null, fingerprintNode, fpData);
        fp2.sources = List.of(root2);
        fp2.persist();
        ValueEntity fp3 = new ValueEntity(null, fingerprintNode, fpData);
        fp3.sources = List.of(root3);
        fp3.persist();
        tm.commit();

        // Create FixedThreshold with min=10, max=100, minInclusive=true, maxInclusive=true
        FixedThreshold ft = new FixedThreshold();
        ft.setMin(10);
        ft.setMax(100);
        ft.setMinInclusive(true);
        ft.setMaxInclusive(true);
        ft.setNodes(fingerprintNode, rootNode, rangeNode);

        // Each root is processed independently (non-cumulative)
        List<ValueEntity> found1 = nodeService.calculateFixedThresholdValues(ft, root1, 0);
        List<ValueEntity> found2 = nodeService.calculateFixedThresholdValues(ft, root2, 0);
        List<ValueEntity> found3 = nodeService.calculateFixedThresholdValues(ft, root3, 0);

        // 5.0 below min=10 → violation
        assertEquals(1, found1.size(), "root1 (5.0) should produce 1 violation (below min)");
        assertEquals("below", found1.getFirst().data.get("direction").asText());
        assertEquals(5.0, found1.getFirst().data.get("value").asDouble());
        assertEquals(10.0, found1.getFirst().data.get("bound").asDouble());

        // 50.0 in range → no violation
        assertEquals(0, found2.size(), "root2 (50.0) should produce no violations");

        // 150.0 above max=100 → violation
        assertEquals(1, found3.size(), "root3 (150.0) should produce 1 violation (above max)");
        assertEquals("above", found3.getFirst().data.get("direction").asText());
        assertEquals(150.0, found3.getFirst().data.get("value").asDouble());
        assertEquals(100.0, found3.getFirst().data.get("bound").asDouble());

        // Verify violation data fields
        for (ValueEntity v : List.of(found1.getFirst(), found3.getFirst())) {
            assertTrue(v.data.has("value"), "result should contain value field");
            assertTrue(v.data.has("bound"), "result should contain bound field");
            assertTrue(v.data.has("direction"), "result should contain direction field");
            assertTrue(v.data.has("fingerprint"), "result should contain fingerprint field");
        }
    }

    @Test
    public void calculateFixedThreshold_with_qvss_data() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        // Load multiple qvss JSON files
        File qvssDir = new File(getClass().getClassLoader().getResource("qvss").getFile());
        List<File> qvssFiles;
        try (Stream<java.nio.file.Path> paths = Files.list(qvssDir.toPath())) {
            qvssFiles = paths.map(java.nio.file.Path::toFile)
                    .filter(f -> f.getName().endsWith(".json"))
                    .sorted()
                    .limit(10)
                    .toList();
        }

        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.name = "upload";
        rootNode.persist();
        JqNode throughputNode = new JqNode("throughput", ".results.\"quarkus3-jvm\".load.avThroughput", rootNode);
        throughputNode.persist();
        JqNode versionNode = new JqNode("version", ".config.QUARKUS_VERSION", rootNode);
        versionNode.persist();

        // Persist fingerprint node and compute fp values per root
        FingerprintNode fpNode = new FingerprintNode("fp", "fp", List.of(versionNode));
        fpNode.persist();
        tm.commit();

        // Load each qvss file, create root + jq values + fingerprint values
        List<ValueEntity> allRootValues = new ArrayList<>();
        int throughputCount = 0;
        for (File f : qvssFiles) {
            JsonNode qvssData = mapper.readTree(f);
            tm.begin();
            ValueEntity rootValue = new ValueEntity(null, rootNode, qvssData);
            rootValue.persist();
            tm.commit();
            allRootValues.add(rootValue);

            Map<String, ValueEntity> sourceValues = Map.of("upload", rootValue);
            tm.begin();
            List<ValueEntity> tpValues = nodeService.calculateJqValues(throughputNode, sourceValues, 0).stream().map(ValueEntity.getEntityManager()::merge).toList();;
            List<ValueEntity> verValues = nodeService.calculateJqValues(versionNode, sourceValues, 0).stream().map(ValueEntity.getEntityManager()::merge).toList();;
            // compute fingerprint values
            if (!verValues.isEmpty()) {
                Map<String, ValueEntity> fpSourceValues = new HashMap<>();
                fpSourceValues.put("version", verValues.getFirst());
                List<ValueEntity> fpValues = nodeService.calculateFpValues(fpNode, fpSourceValues, 0).stream().map(ValueEntity.getEntityManager()::merge).toList();
            }
            tm.commit();

            if (!tpValues.isEmpty()) {
                throughputCount++;
            }
        }
        assertTrue(throughputCount > 0, "should have at least one file with quarkus3-jvm throughput data");

        // Create FixedThreshold with min=10000, max=15000
        FixedThreshold ft = new FixedThreshold();
        ft.setMin(10000);
        ft.setMax(15000);
        ft.setMinInclusive(true);
        ft.setMaxInclusive(true);
        ft.setNodes(fpNode, rootNode, throughputNode);

        int totalViolations = 0;
        for (ValueEntity rootValue : allRootValues) {
            List<ValueEntity> violations = nodeService.calculateFixedThresholdValues(ft, rootValue, 0);
            totalViolations += violations.size();
            for (ValueEntity v : violations) {
                double val = v.data.get("value").asDouble();
                String dir = v.data.get("direction").asText();
                if ("above".equals(dir)) {
                    assertTrue(val > 15000, "above violation should have value > 15000 but was " + val);
                } else if ("below".equals(dir)) {
                    assertTrue(val < 10000, "below violation should have value < 10000 but was " + val);
                }
            }
        }
        assertTrue(totalViolations > 0, "should detect at least one threshold violation across qvss data");
    }

    @Test
    public void calculateFixedThreshold_inclusive_vs_exclusive() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        NodeEntity rootNode = new RootNode();
        rootNode.persist();
        NodeEntity rangeNode = new JqNode("range", ".y", rootNode);
        rangeNode.persist();
        NodeEntity fingerprintNode = new JqNode("fingerprint", ".fingerprint", rootNode);
        fingerprintNode.persist();

        // Range values exactly at boundaries [10.0, 100.0]
        ValueEntity root1 = new ValueEntity(null, rootNode, new TextNode("root1"));
        root1.persist();
        ValueEntity root2 = new ValueEntity(null, rootNode, new TextNode("root2"));
        root2.persist();

        ValueEntity range1 = new ValueEntity(null, rangeNode, DoubleNode.valueOf(10.0));
        range1.sources = List.of(root1);
        range1.persist();
        ValueEntity range2 = new ValueEntity(null, rangeNode, DoubleNode.valueOf(100.0));
        range2.sources = List.of(root2);
        range2.persist();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode fpData = mapper.createObjectNode();
        fpData.put("env", "test");
        ValueEntity fp1 = new ValueEntity(null, fingerprintNode, fpData);
        fp1.sources = List.of(root1);
        fp1.persist();
        ValueEntity fp2 = new ValueEntity(null, fingerprintNode, fpData);
        fp2.sources = List.of(root2);
        fp2.persist();
        tm.commit();

        // both inclusive → no violations (10 >= 10, 100 <= 100)
        FixedThreshold ftInclusive = new FixedThreshold();
        ftInclusive.setMin(10);
        ftInclusive.setMax(100);
        ftInclusive.setMinInclusive(true);
        ftInclusive.setMaxInclusive(true);
        ftInclusive.setNodes(fingerprintNode, rootNode, rangeNode);

        List<ValueEntity> inclusiveR1 = nodeService.calculateFixedThresholdValues(ftInclusive, root1, 0);
        List<ValueEntity> inclusiveR2 = nodeService.calculateFixedThresholdValues(ftInclusive, root2, 0);
        assertEquals(0, inclusiveR1.size(), "minInclusive=true: value 10 should NOT violate min=10");
        assertEquals(0, inclusiveR2.size(), "maxInclusive=true: value 100 should NOT violate max=100");

        // both exclusive → both violate (10 <= 10, 100 >= 100)
        FixedThreshold ftExclusive = new FixedThreshold();
        ftExclusive.setMin(10);
        ftExclusive.setMax(100);
        ftExclusive.setMinInclusive(false);
        ftExclusive.setMaxInclusive(false);
        ftExclusive.setNodes(fingerprintNode, rootNode, rangeNode);

        List<ValueEntity> exclusiveR1 = nodeService.calculateFixedThresholdValues(ftExclusive, root1, 0);
        List<ValueEntity> exclusiveR2 = nodeService.calculateFixedThresholdValues(ftExclusive, root2, 0);
        assertEquals(1, exclusiveR1.size(), "minInclusive=false: value 10 should violate min=10");
        assertEquals(1, exclusiveR2.size(), "maxInclusive=false: value 100 should violate max=100");

        // mixed: minInclusive=false, maxInclusive=true → only min boundary violates
        FixedThreshold ftMixed = new FixedThreshold();
        ftMixed.setMin(10);
        ftMixed.setMax(100);
        ftMixed.setMinInclusive(false);
        ftMixed.setMaxInclusive(true);
        ftMixed.setNodes(fingerprintNode, rootNode, rangeNode);

        List<ValueEntity> mixedR1 = nodeService.calculateFixedThresholdValues(ftMixed, root1, 0);
        List<ValueEntity> mixedR2 = nodeService.calculateFixedThresholdValues(ftMixed, root2, 0);
        assertEquals(1, mixedR1.size(), "minInclusive=false: value 10 should violate min=10");
        assertEquals(0, mixedR2.size(), "maxInclusive=true: value 100 should NOT violate max=100");
    }

    @Test
    public void jsonpathToJq_filter_equals() {
        assertEquals(
                ".boot_time[]?.boot_logs[]? | select(.name == \"early-boot-service.service\")",
                NodeService.jsonpathToJq("$.boot_time[*].boot_logs[*] ? (@.name == \"early-boot-service.service\")"));
    }

    @Test
    public void jsonpathToJq_filter_not_equals() {
        assertEquals(
                ".results[]? | select(.jobName != \"garbage-collection\")",
                NodeService.jsonpathToJq("$.results[*] ?(@.jobName != \"garbage-collection\")"));
    }

    @Test
    public void jsonpathToJq_filter_like_regex() {
        assertEquals(
                ".boot_time[]?.boot_logs[]? | select((.name | test(\"^InitRD$\")))",
                NodeService.jsonpathToJq("$.boot_time[*].boot_logs[*] ? (@.name like_regex \"^InitRD$\")"));
    }

    @Test
    public void jsonpathToJq_filter_with_quoted_fields_and_trailing_path() {
        assertEquals(
                ".faban.summary.benchResults.driverSummary[]? | select(.[\"@name\"] == \"MfgDriver\").customStats.stat[0].passed.[\"text()\"]",
                NodeService.jsonpathToJq("$.faban.summary.benchResults.driverSummary[*] ? (@.\"@name\" == \"MfgDriver\").customStats.stat[0].passed.\"text()\""));
    }

    @Test
    public void jsonpathToJq_no_filter() {
        assertEquals(".foo.bar", NodeService.jsonpathToJq("$.foo.bar"));
        assertEquals(".foo[]?.bar", NodeService.jsonpathToJq("$.foo[*].bar"));
        assertEquals(".foo[]?.bar", NodeService.jsonpathToJq("$.foo.*.bar"));
    }
}
