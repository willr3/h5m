package exp.svc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import exp.FreshDb;
import exp.entity.Node;
import exp.entity.NodeGroup;
import exp.entity.Value;
import exp.entity.node.JqNode;
import exp.entity.node.JsNode;
import exp.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class NodeServiceTest extends FreshDb {

    @Inject
    TransactionManager tm;

    @Inject
    NodeService nodeService;

    @Test
    public void create_without_group() {
        Node jqNode = new JqNode("foo",".bar");
        long response = nodeService.create(jqNode);
        assertTrue(response > 0);
        assertEquals(jqNode.id, response);
    }
    @Test
    public void delete_without_group(){
        Node jqNode = new JqNode("foo",".bar");
        long response = nodeService.create(jqNode);

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
}
