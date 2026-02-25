package io.hyperfoil.tools.h5m.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.entity.Node;
import io.hyperfoil.tools.h5m.entity.NodeGroup;
import io.hyperfoil.tools.h5m.entity.Value;
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

import static io.hyperfoil.tools.h5m.entity.Node.FQDN_SEPARATOR;
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
        Node jqNode = new JqNode("foo",".bar");
        Node response = nodeService.create(jqNode);
        assertNotNull(response.id);
        assertTrue(response.id > 0);
        assertEquals(jqNode.id, response.id);
    }
    @Test
    public void delete_without_group(){
        Node jqNode = new JqNode("foo",".bar");
        Node response = nodeService.create(jqNode);
        nodeService.delete(jqNode);
        List<Node> found = nodeService.findNodeByFqdn("foo");
        assertEquals(0, found.size());
    }

    @Test
    public void delete_does_not_cascade_and_delete_ancestor() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Node aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        tm.commit();

        tm.begin();
        aNode.delete();
        tm.commit();

        Node found = Node.findById(rootNode.id);
        assertNotNull(found);


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
    public void update_changes_javascript_argument_name() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node n = new JqNode("oldName", "operation");
        n.persist();
        Node js = new JsNode("jsNode", "(oldName)=>oldName");
        js.sources=List.of(n);
        js.persist();
        tm.commit();

        n.name = "newName";

        nodeService.update(n);
        Node found = Node.findById(js.id);
        assertEquals("(newName)=>newName", found.operation,"the change should update method");

    }



    @Test
    public void calculateJsValue_yield() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsNode jsNode = new JsNode("js","function* foo(){ yield 1; yield 'foo'; }");
        List<Value> result = nodeService.calculateJsValues(jsNode, Collections.EMPTY_MAP,0);

        assertEquals(2,result.size());
        Value value = result.get(0);
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
        List<Value> result = nodeService.calculateJsValues(jsNode, Collections.EMPTY_MAP,0);

        assertEquals(1,result.size());
        Value value = result.get(0);
        assertNotNull(value);
        JsonNode data = value.data;
        assertNotNull(data);
        assertEquals("123",data.toString());
    }
    @Test
    public void calculateJsValue_arrow_without_parenthesis_text() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.name="root";
        rootNode.persist();
        Value rootValue = new Value(null,rootNode,new TextNode("Bright"));
        rootValue.persist();
        JsNode jsNode = new JsNode("js","root=>'Hi, '+root");
        jsNode.persist();
        tm.commit();

        Map<String,Value> combined = Map.of("root",rootValue);
        List<Value> result = nodeService.calculateJsValues(jsNode, combined,0);

        assertNotNull(result);
        assertEquals(1,result.size());
        Value first = result.get(0);
        assertNotNull(first);
        JsonNode data = first.data;
        assertNotNull(data);
        assertEquals("Hi, Bright",data.asText());
    }

    @Test
    public void calculateSqlJsonpathValues() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        RootNode rootNode = new RootNode();
        rootNode.persist();
        SqlJsonpathNode node = new SqlJsonpathNode("sql","$.buz",List.of(rootNode));//should be a different type of node?
        node.persist();
        Value v1 = new Value();
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

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<Value> calculated = nodeService.calculateSqlJsonpathValues(node,sourceValueMap,0);
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
        Value v1 = new Value();
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

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put(rootNode.name,v1);

        List<Value> calculated = nodeService.calculateSqlJsonpathValues(node,sourceValueMap,0);
        assertNotNull(calculated);
        assertEquals(0,calculated.size());

    }

    @Test
    public void calculateRelativeDifference_root() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Node rangeNode = new JqNode("range",".y",rootNode);
        rangeNode.persist();
        Node domainNode = new JqNode("domain",".domain",rootNode);
        domainNode.persist();
        Node fingerprintNode = new JqNode("fingerprint",".fingerprint",rootNode);
        fingerprintNode.persist();

        Value rootValue01 = new Value(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,new TextNode("root2"));
        rootValue02.persist();
        Value rootValue03 = new Value(null,rootNode,new TextNode("root3"));
        rootValue03.persist();

        Value rangeValue01 = new Value(null,rangeNode, DoubleNode.valueOf(1));
        rangeValue01.sources=List.of(rootValue01);
        rangeValue01.persist();
        Value rangeValue02 = new Value(null,rangeNode, DoubleNode.valueOf(2));
        rangeValue02.sources=List.of(rootValue02);
        rangeValue02.persist();
        Value rangeValue03 = new Value(null,rangeNode, DoubleNode.valueOf(3));
        rangeValue03.sources=List.of(rootValue03);
        rangeValue03.persist();

        //somehow domain values are missing value_edge...
        //LongNode.valueOf breaks this???
        Value domainValue01 = new Value(null,domainNode,DoubleNode.valueOf(10));
        domainValue01.sources=List.of(rootValue01);
        domainValue01.persist();
        Value domainValue02 = new Value(null,domainNode,DoubleNode.valueOf(20));
        domainValue02.sources=List.of(rootValue02);
        domainValue02.persist();
        Value domainValue03 = new Value(null,domainNode, DoubleNode.valueOf(30));
        domainValue03.sources=List.of(rootValue03);
        domainValue03.persist();

        Value fingerprintValue01 = new Value(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue01.sources=List.of(rootValue01);
        fingerprintValue01.persist();
        Value fingerprintValue02 = new Value(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue02.sources=List.of(rootValue02);
        fingerprintValue02.persist();
        Value fingerprintValue03 = new Value(null,fingerprintNode,new TextNode("fp"));
        fingerprintValue03.sources=List.of(rootValue03);
        fingerprintValue03.persist();
        tm.commit();

        RelativeDifference relDifference = new RelativeDifference();
        //relDifference.name="max.y";
        relDifference.setFilter("max");
        relDifference.setWindow(1);
        relDifference.setMinPrevious(1);
        relDifference.setNodes(fingerprintNode,rootNode,rangeNode,domainNode);

        List<Value> found = nodeService.calculateRelativeDifferenceValues(relDifference,rootValue01,0);
        assertNotNull(found);
        assertEquals(1,found.size());
        Value value = found.getFirst();
        System.out.println("found\n"+found);
        System.out.println("value\n"+value+"\n"+value.data);

    }

    @Test
    public void getDependentNodes() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        RootNode root = new RootNode();
        Node n1 = nodeService.create(new JqNode("n1","n1",root));
        Node n11 = nodeService.create(new JqNode("n11","n11",n1));
        Node n12 = nodeService.create(new JqNode("n12","n12",n1));
        Node n121 = nodeService.create(new JqNode("n121","n121",n12));
        tm.commit();

        List<Node> found = nodeService.getDependentNodes(n1);
        assertNotNull(found);
        assertEquals(2,found.size(),"should find two nodes: "+found);
        assertTrue(found.contains(n11),"should find node11 "+n11+" : "+found);
        assertTrue(found.contains(n12),"should find node12 "+n12+" : "+found);
    }

    @Test
    public void calculateJqValues_single_key_sourceValue() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        Node upload = new JqNode();//should be a different type of node?
        upload.name="upload";
        upload.persist();
        Value v1 = new Value();
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

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put("upload",v1);

        JqNode node = new JqNode("foo",".foo");
        List<Value> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(1,calculated.size(),"expect to create a single value from key");
    }
    @Test
    public void calculateJqValues_single_key_iterating() throws IOException, SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        ObjectMapper mapper = new ObjectMapper();
        tm.begin();
        Node upload = new JqNode();//should be a different type of node?
        upload.name="upload";
        upload.persist();
        Value v1 = new Value();
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

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put("upload",v1);

        JqNode node = new JqNode("foo",".foo[]");
        List<Value> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(2,calculated.size(),"expect to create a multiple values from an output file with multiple roots");

        String first = calculated.getFirst().data.toString();
        String second = calculated.getLast().data.toString();

        assertTrue(first.contains("one"),"first returned value should have first match from jq: "+first);
        assertTrue(second.contains("two"),"second returned value should have second match from jq: "+second);
    }

    @Test
    public void calculateJqValues_multiple_sourceValues() throws IOException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Value v1 = new Value();
        v1.data=new TextNode("cat");
        v1.persist();
        Value v2 = new Value();
        v2.data=new TextNode("dog");
        v2.persist();

        JqNode node = new JqNode("foo",".");
        node.persist();

        tm.commit();

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put("v1",v1);
        sourceValueMap.put("v2",v2);

        List<Value> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
        assertEquals(1,calculated.size(),"expect to create a single value from two sources");
        String read = calculated.getFirst().data.toString();
        assertTrue(read.contains("cat"),"first file should be in result: "+read);
        assertTrue(read.contains("dog"),"second file should be in result: "+read);
        assertTrue(read.startsWith("["),"value should be an array: "+read);
        assertTrue(read.endsWith("]"),"value should be an array: "+read);
    }
    @Test
    public void calculateJqValues_multiple_source_order() throws IOException, HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Node node1 = new JqNode();
        node1.name="v1";
        node1.persist();
        Value v1 = new Value();
        v1.data=new TextNode("cat");
        v1.node = node1;
        v1.persist();
        Node node2 = new JqNode();
        node2.name="v2";
        node2.persist();
        Value v2 = new Value();
        v2.data=new TextNode("dog");
        v2.node = node2;
        v2.persist();
        tm.commit();

        Map<String,Value> sourceValueMap = new HashMap<>();
        sourceValueMap.put("v1",v1);
        sourceValueMap.put("v2",v2);

        JqNode node = new JqNode("foo",".");

        node.sources=List.of(node1,node2);

        List<Value> calculated = nodeService.calculateJqValues(node,sourceValueMap,0);
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
        NodeGroup group = new NodeGroup("group1");
        group.persist();
        Node node = new JqNode("node1");
        node.group = group;
        node.persist();
        tm.commit();

        List<Node> found = nodeService.findNodeByFqdn(node.getFqdn());
        assertEquals( 1,found.size());
    }
    @Test
    public void findNodeByFqdn_parent_and_child_name() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeGroup group = new NodeGroup("group1");
        group.persist();
        Node parent = new JqNode("parent");
        parent.group = group;
        parent.persist();

        Node decoy = new JqNode("child",".decoy");
        decoy.group = group;
        decoy.persist();

        Node child = new JqNode("child",".correct");
        child.group = group;
        child.sources=List.of(parent);
        child.persist();

        tm.commit();

        List<Node> found = nodeService.findNodeByFqdn(parent.name+FQDN_SEPARATOR+child.name);
        assertEquals( 1,found.size());
        Node foundNode = found.get(0);
        assertNotNull(foundNode);
        assertEquals(".correct",foundNode.operation);
    }
    @Test
    public void findNodeByFqdn_group_orginal_group_name() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        NodeGroup group = new NodeGroup("group1");
        group.persist();
        NodeGroup group2 = new NodeGroup("group2");
        group2.persist();
        Node node = new JqNode("node1");
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
        Node upload = new RootNode();
        upload.name="upload";
        upload.persist();

        Value v1 = new Value();
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

        Node foo = new JqNode();
        foo.name="foo";
        foo.operation=".foo";
        foo.sources=List.of(upload);
        foo.persist();
        tm.commit();

        List<Value> calculated = nodeService.calculateValues(foo,List.of(v1));
        tm.begin();
        assertEquals(1,calculated.size(),"expected to calculate one value from foo:"+calculated);

        Value calculatedFoo = calculated.get(0);
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
        Node upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        Value v1 = new Value();
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

        Node foo = new JqNode("foo",".foo[]",upload);
        foo.persist();

        Node biz = new JqNode("biz",".biz",upload);
        biz.scalarMethod= Node.ScalarVariableMethod.First;
        biz.persist();

        Node buz = new JqNode("buz",".buz",upload);
        buz.scalarMethod= Node.ScalarVariableMethod.All;
        buz.persist();

        Node combined = new JqNode("combined",".",foo,biz,buz);
        combined.persist();
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo,List.of(v1)).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(biz,List.of(v1)).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(buz,List.of(v1)).forEach(Value.getEntityManager()::merge);
        tm.commit();
        List<Value> calculated = nodeService.calculateValues(combined,List.of(v1));

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
        Node upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        Value v1 = new Value();
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

        Node foo = new JqNode("foo",".foo[]",upload);
        foo.persist();

        Node bar = new JqNode("bar",".bar[]",upload);
        bar.persist();

        Node biz = new JqNode("biz",".biz",upload);
        biz.scalarMethod= Node.ScalarVariableMethod.First;
        biz.persist();

        Node buz = new JqNode("buz",".buz",upload);
        buz.scalarMethod= Node.ScalarVariableMethod.All;
        buz.persist();

        Node combined = new JqNode("combined",".",foo,bar,biz,buz);
        combined.multiType= Node.MultiIterationType.NxN;
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo,List.of(v1)).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(bar,List.of(v1)).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(biz,List.of(v1)).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(buz,List.of(v1)).forEach(Value.getEntityManager()::merge);
        tm.commit();
        List<Value> calculated = nodeService.calculateValues(combined,List.of(v1));

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
        assertTrue(Stream.of("one","uno","cat","dog").allMatch(first::contains),"missing expected key in "+first);
        assertTrue(Stream.of("one","dos","dog").allMatch(second::contains),"missing expected key in "+second);
        assertTrue(Stream.of("two","uno","dog").allMatch(third::contains),"missing expected key in "+third);
        assertTrue(Stream.of("two","dos","dog").allMatch(fourth::contains),"missing expected key in "+fourth);

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
        Node upload = new JqNode("upload");//should be a different type of node?
        upload.persist();

        Value v1 = new Value();
        v1.data = mapper.readTree("""
                {
                  "foo": [ { "key": "one"}, { "key" : "two" } ],
                  "bar": [ { "k": "uno" }, { "k": "dos"}, { "k": "tres"} ],
                  "biz": [ { "j": "ant" }, { "j": "bee"}, { "j": "cat"} ]
                }
                """);
        v1.node = upload;
        v1.persist();

        Node foo = new JqNode("foo", ".foo[]", upload);
        foo.persist();

        Node bar = new JqNode("bar", ".bar[]", upload);
        bar.persist();

        Node biz = new JqNode("biz", ".biz[]", upload);
        biz.scalarMethod = Node.ScalarVariableMethod.First;
        biz.persist();

        Node combined = new JqNode("combined", ".", foo, bar, biz);
        combined.multiType = Node.MultiIterationType.NxN;
        tm.commit();
        tm.begin();
        nodeService.calculateValues(foo, List.of(v1) ).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(bar, List.of(v1) ).forEach(Value.getEntityManager()::merge);
        nodeService.calculateValues(biz, List.of(v1) ).forEach(Value.getEntityManager()::merge);
        tm.commit();

        List<Value> calculated = nodeService.calculateValues(combined,List.of(v1));
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
        Node rootNode = new RootNode();
        rootNode.persist();
        JqNode platformNode = new JqNode("platform",".platform",rootNode);
        platformNode.persist();
        JqNode buildTypeNode = new JqNode("buildType",".buildType",rootNode);
        buildTypeNode.persist();
        tm.commit();

        FingerprintNode fpNode = new FingerprintNode("fp","fp",List.of(platformNode, buildTypeNode));

        Value platformValue = new Value(null, platformNode, new TextNode("x86"));
        Value buildTypeValue = new Value(null, buildTypeNode, new TextNode("release"));

        Map<String,Value> sourceValues = new HashMap<>();
        sourceValues.put("platform", platformValue);
        sourceValues.put("buildType", buildTypeValue);

        List<Value> result = nodeService.calculateFpValues(fpNode, sourceValues, 0);

        assertEquals(1, result.size(), "should produce exactly one fingerprint value");
        Value fpValue = result.getFirst();
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
        Node rootNode = new RootNode();
        rootNode.persist();
        // define sources in reverse alphabetical order: platform before buildType
        JqNode platformNode = new JqNode("platform",".platform",rootNode);
        platformNode.persist();
        JqNode buildTypeNode = new JqNode("buildType",".buildType",rootNode);
        buildTypeNode.persist();
        tm.commit();

        FingerprintNode fpNode = new FingerprintNode("fp","fp",List.of(platformNode, buildTypeNode));

        Value platformValue = new Value(null, platformNode, new TextNode("x86"));
        Value buildTypeValue = new Value(null, buildTypeNode, new TextNode("release"));

        Map<String,Value> sourceValues = new HashMap<>();
        sourceValues.put("platform", platformValue);
        sourceValues.put("buildType", buildTypeValue);

        List<Value> result = nodeService.calculateFpValues(fpNode, sourceValues, 0);

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
    public void calculateRelativeDifference_with_fingerprint_filter() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Node rangeNode = new JqNode("range",".y",rootNode);
        rangeNode.persist();
        Node domainNode = new JqNode("domain",".domain",rootNode);
        domainNode.persist();
        Node fingerprintNode = new JqNode("fingerprint",".fingerprint",rootNode);
        fingerprintNode.persist();

        // create 3 root values for x86 and 3 for arm
        Value rootX86_01 = new Value(null,rootNode,new TextNode("rootX86_1"));
        rootX86_01.persist();
        Value rootX86_02 = new Value(null,rootNode,new TextNode("rootX86_2"));
        rootX86_02.persist();
        Value rootX86_03 = new Value(null,rootNode,new TextNode("rootX86_3"));
        rootX86_03.persist();

        Value rootArm_01 = new Value(null,rootNode,new TextNode("rootArm_1"));
        rootArm_01.persist();
        Value rootArm_02 = new Value(null,rootNode,new TextNode("rootArm_2"));
        rootArm_02.persist();
        Value rootArm_03 = new Value(null,rootNode,new TextNode("rootArm_3"));
        rootArm_03.persist();

        // x86 range values
        Value rangeX86_01 = new Value(null,rangeNode, DoubleNode.valueOf(1));
        rangeX86_01.sources=List.of(rootX86_01);
        rangeX86_01.persist();
        Value rangeX86_02 = new Value(null,rangeNode, DoubleNode.valueOf(2));
        rangeX86_02.sources=List.of(rootX86_02);
        rangeX86_02.persist();
        Value rangeX86_03 = new Value(null,rangeNode, DoubleNode.valueOf(3));
        rangeX86_03.sources=List.of(rootX86_03);
        rangeX86_03.persist();

        // arm range values
        Value rangeArm_01 = new Value(null,rangeNode, DoubleNode.valueOf(10));
        rangeArm_01.sources=List.of(rootArm_01);
        rangeArm_01.persist();
        Value rangeArm_02 = new Value(null,rangeNode, DoubleNode.valueOf(20));
        rangeArm_02.sources=List.of(rootArm_02);
        rangeArm_02.persist();
        Value rangeArm_03 = new Value(null,rangeNode, DoubleNode.valueOf(30));
        rangeArm_03.sources=List.of(rootArm_03);
        rangeArm_03.persist();

        // x86 domain values
        Value domainX86_01 = new Value(null,domainNode,DoubleNode.valueOf(10));
        domainX86_01.sources=List.of(rootX86_01);
        domainX86_01.persist();
        Value domainX86_02 = new Value(null,domainNode,DoubleNode.valueOf(20));
        domainX86_02.sources=List.of(rootX86_02);
        domainX86_02.persist();
        Value domainX86_03 = new Value(null,domainNode,DoubleNode.valueOf(30));
        domainX86_03.sources=List.of(rootX86_03);
        domainX86_03.persist();

        // arm domain values
        Value domainArm_01 = new Value(null,domainNode,DoubleNode.valueOf(100));
        domainArm_01.sources=List.of(rootArm_01);
        domainArm_01.persist();
        Value domainArm_02 = new Value(null,domainNode,DoubleNode.valueOf(200));
        domainArm_02.sources=List.of(rootArm_02);
        domainArm_02.persist();
        Value domainArm_03 = new Value(null,domainNode,DoubleNode.valueOf(300));
        domainArm_03.sources=List.of(rootArm_03);
        domainArm_03.persist();

        // x86 fingerprint values (structured ObjectNode)
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode x86Fp = mapper.createObjectNode();
        x86Fp.put("platform", "x86");
        Value fpX86_01 = new Value(null,fingerprintNode, x86Fp);
        fpX86_01.sources=List.of(rootX86_01);
        fpX86_01.persist();
        Value fpX86_02 = new Value(null,fingerprintNode, x86Fp);
        fpX86_02.sources=List.of(rootX86_02);
        fpX86_02.persist();
        Value fpX86_03 = new Value(null,fingerprintNode, x86Fp);
        fpX86_03.sources=List.of(rootX86_03);
        fpX86_03.persist();

        // arm fingerprint values (structured ObjectNode)
        ObjectNode armFp = mapper.createObjectNode();
        armFp.put("platform", "arm");
        Value fpArm_01 = new Value(null,fingerprintNode, armFp);
        fpArm_01.sources=List.of(rootArm_01);
        fpArm_01.persist();
        Value fpArm_02 = new Value(null,fingerprintNode, armFp);
        fpArm_02.sources=List.of(rootArm_02);
        fpArm_02.persist();
        Value fpArm_03 = new Value(null,fingerprintNode, armFp);
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

        List<Value> found = nodeService.calculateRelativeDifferenceValues(relDifference,rootX86_01,0);
        assertNotNull(found);
        assertFalse(found.isEmpty(), "x86 filtered results should not be empty");

        // verify that all filtered results contain x86 domain values (10, 20, 30)
        Set<Double> x86DomainValues = Set.of(10.0, 20.0, 30.0);
        for (Value v : found) {
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

        List<Value> armNoFilter = nodeService.calculateRelativeDifferenceValues(relDiffNoFilter,rootArm_01,0);
        assertFalse(armNoFilter.isEmpty(), "arm unfiltered results should not be empty");
        // verify arm results contain arm domain values (100, 200, 300)
        Set<Double> armDomainValues = Set.of(100.0, 200.0, 300.0);
        for (Value v : armNoFilter) {
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

        List<Value> armWithX86Filter = nodeService.calculateRelativeDifferenceValues(relDiffArmWithX86Filter,rootArm_01,0);
        assertTrue(armWithX86Filter.isEmpty(),
            "arm root with x86 filter should produce no results but found " + armWithX86Filter.size());
    }
}
