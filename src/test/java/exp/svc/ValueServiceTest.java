package exp.svc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import exp.FreshDb;
import exp.entity.Node;
import exp.entity.Value;
import exp.entity.node.JqNode;
import exp.entity.node.RootNode;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.hibernate.LazyInitializationException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class ValueServiceTest extends FreshDb {

    @Inject
    ValueService valueService;

    @Inject
    TransactionManager tm;


    @Test
    public void writeToFile_string() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, IOException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Value rootValue = new Value(null,rootNode,new TextNode("a"));
        rootValue.persist();
        tm.commit();

        File f = Files.createTempFile("h5m-test", ".writeToFile").toFile();
        f.deleteOnExit();
        valueService.writeToFile(rootValue.id,f.getAbsolutePath());

        String content = Files.readString(f.toPath());

        assertNotNull(content);
        assertTrue(content.contains("a"));
        assertEquals("\"a\"", content);
        //TODO should string content include the quotes?
    }

    @Test
    public void delete_does_not_cascade_and_delete_ancestor() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Node aNode = new JqNode("a",".a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        Value rootValue = new Value(null,rootNode,new TextNode("a"));
        rootValue.persist();
        Value aValue = new Value(null,aNode,new TextNode("a"));
        aValue.sources=List.of(rootValue);
        aValue.persist();
        tm.commit();

        tm.begin();
        aValue.delete();
        tm.commit();

        Value found = Value.findById(rootValue.id);
        assertNotNull(found);
    }


    @Test
    public void lazy_data() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException, JsonProcessingException {
        tm.begin();
        ObjectMapper objectMapper = new ObjectMapper();
        Node rootNode = new RootNode();
        rootNode.persist();
        Value rootValue01 = new Value(null,rootNode,objectMapper.readTree("{\"this\":{ \"is\":{\"silly\":\"yes\"}}}"));
        rootValue01.persist();
        tm.commit();

        rootValue01 = null;

        List<Value> found = valueService.getValues(rootNode);
        assertEquals(1, found.size());
        System.out.println(found.size());
        for(Value v : found){
            assertThrows(LazyInitializationException.class, () -> {
                assertNull(v.data); // Access the lazy-loaded attribute
            });
        }


    }


    @Test
    public void findMatchingFingerprint_deep_ancestry_json_value() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist();
        Node aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist();
        Node abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();
        Node abcNode = new JqNode("abc");
        abcNode.sources=List.of(abNode);
        abcNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        Value rootValue01 = new Value(null,rootNode,objectMapper.readTree("111"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,objectMapper.readTree("222"));
        rootValue02.persist();

        Value aValue01 = new Value(null,aNode,objectMapper.readTree("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        Value aValue02 = new Value(null,aNode,objectMapper.readTree("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        Value abValue01 = new Value(null,abNode,objectMapper.readTree("1"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        Value abValue02 = new Value(null,abNode,objectMapper.readTree("2"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();

        Value abcValue01 = new Value(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));//matching value to find fingerprint
        abcValue01.sources=List.of(abValue01);
        abcValue01.persist();
        Value abcValue02 = new Value(null,abcNode,objectMapper.readTree("{\"a\":123,\"b\":456}"));
        abcValue02.sources=List.of(abValue02);
        abcValue02.persist();
        tm.commit();


        List<Value> found = valueService.findMatchingFingerprint(aNode,abcValue01);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(aValue01),found.toString());
        assertTrue(found.contains(aValue02),found.toString());

    }
    @Test
    public void findMatchingFingerprint_integer_value() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist(rootNode);
        Node aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist(aNode);
        Node abNode = new JqNode("ab");
        abNode.sources=List.of(aNode);
        abNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        Value rootValue01 = new Value(null,rootNode,objectMapper.readTree("111"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,objectMapper.readTree("222"));
        rootValue02.persist();

        Value aValue01 = new Value(null,aNode,objectMapper.readTree("11"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        Value aValue02 = new Value(null,aNode,objectMapper.readTree("22"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        Value abValue01 = new Value(null,abNode,objectMapper.readTree("67"));
        abValue01.sources=List.of(aValue01);
        abValue01.persist();
        Value abValue02 = new Value(null,abNode,objectMapper.readTree("67"));
        abValue02.sources=List.of(aValue02);
        abValue02.persist();
        tm.commit();


        List<Value> found = valueService.findMatchingFingerprint(aNode,abValue01);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(aValue01),found.toString());
        assertTrue(found.contains(aValue02),found.toString());

    }
    @Test
    public void findMatchingFingerprintOrderBy() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist(rootNode);
        Node aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist(aNode);
        Node bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        Value rootValue01 = new Value(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        Value aValue01 = new Value(null,aNode,new TextNode("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        Value aValue02 = new Value(null,aNode,new TextNode("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        Value bValue01 = new Value(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        Value bValue02 = new Value(null,bNode,new TextNode("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();
        tm.commit();


        List<Value> found = valueService.findMatchingFingerprintOrderBy(rootNode,aValue01,bNode);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(rootValue01),found.toString());
        assertTrue(found.contains(rootValue02),found.toString());

        assertEquals(rootValue01,found.get(0),found.toString());
        assertEquals(rootValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprintOrderBy_sibling() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist(rootNode);
        Node aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist(aNode);
        Node bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();
        Node cNode = new JqNode("c");
        cNode.sources=List.of(rootNode);
        cNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        Value rootValue01 = new Value(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        Value aValue01 = new Value(null,aNode,new TextNode("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        Value aValue02 = new Value(null,aNode,new TextNode("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        Value bValue01 = new Value(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        Value bValue02 = new Value(null,bNode,new TextNode("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();


        Value cValue01 = new Value(null,cNode,new TextNode("c1"));
        cValue01.sources=List.of(rootValue01);
        cValue01.persist();
        Value cValue02 = new Value(null,cNode,new TextNode("c2"));
        cValue02.sources=List.of(rootValue02);
        cValue02.persist();


        tm.commit();


        List<Value> found = valueService.findMatchingFingerprintOrderBy(cNode,aValue01,bNode);

        System.out.println(found);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(cValue01),found.toString());
        assertTrue(found.contains(cValue02),found.toString());

        assertEquals(cValue01,found.get(0),found.toString());
        assertEquals(cValue02,found.get(1),found.toString());

    }
    @Test
    public void findMatchingFingerprintOrderBy_cousin() throws SystemException, NotSupportedException, JsonProcessingException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node rootNode = new RootNode();
        rootNode.persist(rootNode);
        Node aNode = new JqNode("a");
        aNode.sources=List.of(rootNode);
        aNode.persist(aNode);
        Node bNode = new JqNode("b");
        bNode.sources=List.of(rootNode);
        bNode.persist();
        Node cNode = new JqNode("c");
        cNode.sources=List.of(rootNode);
        cNode.persist();
        Node caNode = new JqNode("ca");
        caNode.sources=List.of(cNode);
        caNode.persist();
        ObjectMapper objectMapper = new ObjectMapper();

        Value rootValue01 = new Value(null,rootNode,new TextNode("root1"));
        rootValue01.persist();
        Value rootValue02 = new Value(null,rootNode,new TextNode("root2"));
        rootValue02.persist();

        Value aValue01 = new Value(null,aNode,new TextNode("a"));
        aValue01.sources=List.of(rootValue01);
        aValue01.persist();

        Value aValue02 = new Value(null,aNode,new TextNode("a"));
        aValue02.sources=List.of(rootValue02);
        aValue02.persist();

        Value bValue01 = new Value(null,bNode,new TextNode("b1"));
        bValue01.sources=List.of(rootValue01);
        bValue01.persist();
        Value bValue02 = new Value(null,bNode,new TextNode("b2"));
        bValue02.sources=List.of(rootValue02);
        bValue02.persist();


        Value cValue01 = new Value(null,cNode,new TextNode("c1"));
        cValue01.sources=List.of(rootValue01);
        cValue01.persist();
        Value cValue02 = new Value(null,cNode,new TextNode("c2"));
        cValue02.sources=List.of(rootValue02);
        cValue02.persist();

        Value caValue01 = new Value(null,caNode,new TextNode("ca1"));
        caValue01.sources=List.of(cValue01);
        caValue01.persist();
        Value caValue02 = new Value(null,caNode,new TextNode("ca2"));
        caValue02.sources=List.of(cValue02);
        caValue02.persist();

        tm.commit();


        List<Value> found = valueService.findMatchingFingerprintOrderBy(caNode,aValue01,bNode);

        System.out.println(found);

        assertNotNull(found);
        assertEquals(2,found.size(),found.toString());
        assertTrue(found.contains(caValue01),found.toString());
        assertTrue(found.contains(caValue02),found.toString());

        assertEquals(caValue01,found.get(0),found.toString());
        assertEquals(caValue02,found.get(1),found.toString());

    }

    @Test
    public void persist_fixes_source_order() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node root = new RootNode();
        root.persist();
        Node a = new JqNode("a");
        a.sources=List.of(root);
        Node ab = new JqNode("ab");
        ab.sources=List.of(a);
        ab.persist();
        Node ac = new JqNode("ac");
        ac.sources=List.of(a);
        Node abc = new JqNode("abc");
        abc.sources=List.of(ab,ac,a,root);
        abc.persist();

        Value vr = new Value();
        vr.persist();
        Value va = new Value();
        va.sources=List.of(vr);
        va.persist();
        Value vab = new Value();
        vab.sources=List.of(va);
        vab.persist();
        Value vac = new Value();
        vac.sources=List.of(va);
        vac.persist();
        Value vabc =  new Value();
        vabc.sources=List.of(vab,vac,va,vr);
        vabc.persist();
        tm.commit();

        tm.begin();
        try{
            Value found = valueService.byId(vabc.id);
            List<Value> sources = found.sources;
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
        Node root = new RootNode();
        root.persist();
        Node alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        Node bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        Node bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        Value rootValue = new Value();
        rootValue.node = root;
        rootValue.persist();

        Value alphaValue = new Value();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        Value bravoValue = new Value();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        Value bravobravoValue = new Value();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();
        tm.commit();

        List<Value> values = valueService.getValues(root);
        assertNotNull(values,"result should not be null");
        assertEquals(1,values.size(),"result should contain 1 value: "+values);
        assertTrue(values.contains(rootValue),"result should contain root value: "+values);

    }
    @Test
    public void getDescendantValues_value_node() throws HeuristicRollbackException, SystemException, HeuristicMixedException, RollbackException, NotSupportedException {
        tm.begin();
        Node root = new JqNode("root");
        root.persist();
        Node alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        Node bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        Node bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        Value rootValue = new Value();
        rootValue.node = root;
        rootValue.persist();

        Value alphaValue = new Value();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        Value bravoValue = new Value();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        Value bravobravoValue = new Value();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<Value> found = valueService.getDescendantValues(rootValue,bravobravo);

        assertFalse(found.contains(rootValue),"descendants should not include self: "+found);
        assertTrue(found.contains(bravobravoValue),"descendants should include value from target node: "+found);
    }

    @Test
    public void getDescendantValues_node() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node root = new JqNode("root");
        root.persist();
        Node alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        Node bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        Node bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(bravo);
        bravobravo.persist();

        Value rootValue = new Value();
        rootValue.node = root;
        rootValue.persist();

        Value alphaValue = new Value();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        Value bravoValue = new Value();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        Value bravobravoValue = new Value();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<Value> found = valueService.getDescendantValues(root);

        assertEquals(3,found.size(),"expect to see three entries");
        assertTrue(found.contains(bravobravoValue),"missing bravobravo["+bravobravoValue.id+"]'s value: "+found);
    }

    @Test
    public void getDescendantValues_two_generations() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node root = new JqNode("root");
        root.persist();
        Node alpha = new JqNode("alpha");
        alpha.sources=List.of(root);
        alpha.persist();
        Node bravo = new JqNode("bravo");
        bravo.sources=List.of(root);
        bravo.persist();
        Node bravobravo = new JqNode("bravobravo");
        bravobravo.sources=List.of(root,bravo);
        bravobravo.persist();

        Value rootValue = new Value();
        rootValue.node = root;
        rootValue.persist();

        Value alphaValue = new Value();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        Value bravoValue = new Value();
        bravoValue.node = bravo;
        bravoValue.sources = List.of(rootValue);
        bravoValue.persist();

        Value bravobravoValue = new Value();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue);
        bravobravoValue.persist();

        tm.commit();

        List<Value> found = valueService.getDescendantValues(rootValue);
        assertEquals(3,found.size(),"expect to see three entries");
        assertTrue(found.contains(bravobravoValue),"missing bravobravo["+bravobravoValue.id+"]'s value: "+found);
        assertFalse(found.contains(rootValue),"root value should not be in results: "+found);
    }
    @Test
    public void getDirectDescendantValues() throws SystemException, NotSupportedException, HeuristicRollbackException, HeuristicMixedException, RollbackException {
        tm.begin();
        Node root = new JqNode("root");
        root.persist();
        Node alpha = new JqNode("alpha");
        alpha.persist();
        Node bravo = new JqNode("bravo");
        bravo.persist();
        Node bravobravo = new JqNode("bravobravo");
        bravobravo.persist();

        Value rootValue = new Value();
        rootValue.node = root;
        rootValue.persist();

        Value alphaValue = new Value();
        alphaValue.node = alpha;
        alphaValue.sources = List.of(rootValue);
        alphaValue.persist();

        Value bravoValue1 = new Value();
        bravoValue1.node = bravo;
        bravoValue1.sources = List.of(rootValue);
        bravoValue1.persist();

        Value bravoValue2 = new Value();
        bravoValue2.node = bravo;
        bravoValue2.sources = List.of(rootValue);
        bravoValue2.persist();

        Value bravobravoValue = new Value();
        bravobravoValue.node = bravobravo;
        bravobravoValue.sources = List.of(bravoValue1);
        bravobravoValue.persist();

        tm.commit();

        List<Value> found = valueService.getDirectDescendantValues(rootValue,bravo);
        assertEquals(2,found.size(),"expect to see 2 entry: "+found);
        assertFalse(found.contains(bravobravoValue),"should not contain bravobravo["+bravobravoValue.id+"]'s value: "+found);
    }
}
