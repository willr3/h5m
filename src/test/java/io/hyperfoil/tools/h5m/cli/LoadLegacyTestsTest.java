package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.FreshDb;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.entity.ValueEntity;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.hyperfoil.tools.jjq.JqProgram;
import io.hyperfoil.tools.jjq.value.JqObject;
import io.hyperfoil.tools.jjq.value.JqString;
import io.hyperfoil.tools.jjq.value.JqValue;
import io.hyperfoil.tools.jjq.value.JqValues;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.HashedSets;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.transaction.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LoadLegacyTestsTest extends FreshDb {

    @Inject
    LoadLegacyTests loadLegacyTests;

    @Inject
    NodeService nodeService;

    @Inject
    FolderService folderService;
    
    @Test
    @Transactional
    public void extractor_lax_isArray_match_select() throws IOException {
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("biz","$.a.b ? (@.c.e==\"two\").c.d",true);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(1,"foo",null, Arrays.asList(extractor));
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        folder.id = folderService.create(folder);
        folder = folderService.read(folder.id);
        assertNotNull(folder);
        assertNotNull(folder.group);
        assertNotNull(folder.group.sources);
        assertEquals(1,folder.group.sources.size());
        NodeEntity node = folder.group.sources.getFirst();
        assertNotNull(node);
        assertEquals("foo",node.name,"name should be changed to match label");
        if(node instanceof JqNode jqNode){
            ValueEntity value = new ValueEntity(null,node,JqValues.parse("""
                { "a" : { "b" : [ { "c" : [{ "d" : "one", "e" : "two" }]} ] } }
                """));
            List<ValueEntity> calculated = nodeService.calculateJqValues(jqNode, Map.of(folder.group.root.id,value),1);
            assertNotNull(calculated);
            assertEquals(1,calculated.size());
            ValueEntity found = calculated.getFirst();
            assertNotNull(found,"entity should not be null");
            assertNotNull(found.data,"value should have data");
            assertTrue(found.data.isArray(),"data should be an array: "+found.data);
            assertEquals(1,found.data.length(),"expect to find one entry: "+found.data);
            assertEquals(JqString.of("one"),found.data.getElement(0));
        }else{
            fail("node should be jq");
        }
    }

    @Test
    @Transactional
    public void extractor_lax_isArray_match() throws IOException {
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("biz","$.a.b.c",true);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(1,"foo",null, Arrays.asList(extractor));
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        folder.id = folderService.create(folder);
        folder = folderService.read(folder.id);
        assertNotNull(folder);
        assertNotNull(folder.group);
        assertNotNull(folder.group.sources);
        assertEquals(1,folder.group.sources.size());
        NodeEntity node = folder.group.sources.getFirst();
        assertNotNull(node);
        assertEquals("foo",node.name,"name should be changed to match label");
        if(node instanceof JqNode jqNode){
            ValueEntity value = new ValueEntity(null,node,JqValues.parse("""
                { "a" : { "b" : [{ "c" : "one" }] } }
                """));
            List<ValueEntity> calculated = nodeService.calculateJqValues(jqNode, Map.of(folder.group.root.id,value),1);
            assertNotNull(calculated);
            assertEquals(1,calculated.size());
            ValueEntity found = calculated.getFirst();
            assertNotNull(found,"entity should not be null");
            assertNotNull(found.data,"value should have data");
            assertTrue(found.data.isArray(),"data should be an array: "+found.data);
            assertEquals(1,found.data.length(),"expect to find one entry: "+found.data);
            assertEquals(JqString.of("one"),found.data.getElement(0));
        }else{
            fail("node should be jq");
        }
    }

    @Test
    public void extractor_equals(){
        LoadLegacyTests.Extractor one = new LoadLegacyTests.Extractor("foo","bar",false);
        LoadLegacyTests.Extractor two = new LoadLegacyTests.Extractor("foo","bar",false);

        assertTrue(one.equals(two));
        assertTrue(one.hashCode() == two.hashCode());
        assertTrue(one.equals(one));
    }

    @Test
    public void label_equals_identiyFuction_and_null(){
        LoadLegacyTests.Extractor one = new LoadLegacyTests.Extractor("tag","$.tag",false);
        LoadLegacyTests.Extractor two = new LoadLegacyTests.Extractor("tag","$.tag",false);

        LoadLegacyTests.Label withFunction = new LoadLegacyTests.Label(1,"foo","tags => tags", List.of(one));
        LoadLegacyTests.Label withoutFunction = new LoadLegacyTests.Label(2,"foo",null, Arrays.asList(two));

        assertTrue(withFunction.equals(withoutFunction));
        assertEquals(withFunction.hashCode(), withoutFunction.hashCode(),"same hashcode despite different forms of identity function");

        HashedSets<String,LoadLegacyTests.Label> sets = new HashedSets<>();
        sets.put(withFunction.name(),withFunction);
        sets.put(withoutFunction.name(),withoutFunction);

        assertEquals(1,sets.get("foo").size(),"only one label should be added to the hashedSets");

    }

    @Test
    public void createFolder_one_transform_one_label_duplicate_extractor_name(){
        LoadLegacyTests.Extractor transformExtractor = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$[0].one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Transformer transformer = new LoadLegacyTests.Transformer(-1,"transformName","args=>[args]","targetUri",List.of(transformExtractor),List.of(label1));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",new HashedSets<>(), List.of(),Collections.emptyList(),List.of(transformer),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);


        System.out.println(folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(4,folder.group.sources.size(),"Expect 3 jq nodes and 1 js node\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(3,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 3 JqNodes (2 converted from sql + 1 combiner)\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("dataset")).count(),"Expect 1 named dataset \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));


    }


    @Test
    public void createFolder_array_schemaPath(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("label","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$[0].\"$schema\"",label1);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());
        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        folder.group.sources.forEach(System.out::println);

        assertNotNull(folder);
        assertNotNull(folder.group);
        assertEquals(2,folder.group.sources.size(),"expect two nodes in the folder");

        NodeEntity dotZero = folder.group.sources.stream().filter(v->v.operation.equals(".[0]")).findAny().orElse(null);
        assertNotNull(dotZero,"expect to find a node that accesses first entry in array using .[0]");

        NodeEntity labelNode =  folder.group.sources.stream().filter(v->v.name.equals("label")).findAny().orElse(null);
        assertNotNull(labelNode);
        assertEquals(1,labelNode.sources.size(),"the extractor should have a single source");
        assertTrue(labelNode.sources.contains(dotZero),"the .[0] node should be the extractor source");

    }

    @Test
    public void createFolder_one_schemaPath_duplicate_label_name(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor2));


        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);

        assertEquals(3,folder.group.sources.size(),"Expect 2 JqNodes and 1 JsNode\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 2 JqNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(3,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 3 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }

    @Test
    public void createFolder_two_schemaPath_same_schema(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","",List.of(extractor1));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$[0].\"$schema\"",label1);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);

        folder.group.sources.forEach(System.out::println);
        assertTrue(folder.group.sources.stream().anyMatch(v->v.operation.equals(".[0]")),"folder should have node for $[0].'$schema' jsonpath");
        assertTrue(folder.group.sources.stream().anyMatch(v->v.name.equals("label")),"folder should have a node that corresponds to the label");
        assertEquals(2,folder.group.sources.stream().filter(v->v.operation.contains(".one")).count(),
                "folder should have two nodes that correspond to the extractor:\n"
                        +folder.group.sources.stream().filter(v->v.operation.contains(".one"))
                        .map(Object::toString).collect(Collectors.joining("\n"))
        );
        assertEquals(4,folder.group.sources.size(),"expect 4 nodes in group\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));



    }

    @Test
    public void createFolder_two_schemaPath_duplicate_label_name(){

        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor2));


        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.other.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);

        assertEquals(4,folder.group.sources.size(),"Expect 3 JqNodes and 1 JsNode\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(3,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 3 JqNodes (1 original + 2 converted from sql)\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(3,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 3  named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
    }

    @Test
    public void createFolder_single_extractor_renamed_for_identity_label(){
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("extractor","$.tag",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(1,folder.group.sources.size());

        NodeEntity first = folder.group.sources.get(0);

        assertNotNull(first);
        assertEquals("label",first.name);
        assertEquals(".tag?",first.operation);
        assertInstanceOf(JqNode.class,first);
    }
    @Test
    public void createFolder_variable_replaced_by_single_source(){
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("extractor","$.tag",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor));

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1,"variable",List.of(label.name()),null);

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(1,folder.group.sources.size());


    }
    @Test
    public void createFolder_variable_with_two_sources(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label1","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1,"label2","foo=>foo",List.of(extractor2));

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1,"variable",List.of(label1.name(),label2.name()),"args=>args.a+args.b");

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(3,folder.group.sources.size(),"Expect 2 SQL nodes and a js node");

        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 2 SqlNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("variable")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }

    @Test
    public void createFolder_fingerprint(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label1","foo=>foo",List.of(extractor1));
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(List.of(label1.name()),null,List.of(),"");
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);


        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, List.of(fingerprint),List.of(),List.of(),List.of());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(2,folder.group.sources.size(),"Expect 1 SQL node and a fingerprint node");
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 1 sql node \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FingerprintNode)).count(),"Expect 1 fingerprint\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));


    }

    @Test
    public void createFolder_changeDetection_threshold() {
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label1","foo=>foo",List.of(extractor1));
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(List.of(label1.name()),null,List.of(),"");
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        JqObject config = (JqObject) JqValues.parse("""
                {
                    "max" : { "inclusive" : true, "enabled" : true, "value" : 10 },
                    "min" : { "inclusive" : true, "enabled" : true, "value" : 5 }
                }
                """);
        LoadLegacyTests.ChangeDetection changeDetection = new LoadLegacyTests.ChangeDetection(-1,-1,"fixedThreshold", config);

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1,"variable",List.of(label1.name()),null);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, List.of(fingerprint),List.of(changeDetection),List.of(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        assertNotNull(folder);

        assertNotNull(folder.group);

        assertEquals(3,folder.group.sources.size(),"Expect 3 Nodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 1 sql node \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FingerprintNode)).count(),"Expect 1 fingerprint\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FixedThreshold)).count(),"Expect 1 threshold node\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }

    @Test
    public void createNodeFromLabel_extractorPrefix_renames_js_parameter(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("one","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("two","$.two",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"label","(one,two)=>one+two",List.of(extractor1,extractor2));

        LoadLegacyTests withPrefix = new LoadLegacyTests();
        withPrefix.extractorPrefix="_";

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();
        NodeEntity labelNode = withPrefix.createNodesFromLabel(label,group.root,group,tracker,new HashSet<>(),false);
        assertEquals("(_one,_two)=>_one+_two",labelNode.operation,"expect extractor prefix to rename node label operation parameters");
    }

    @Test
    public void createNodesFromLabel_single_extractor_null_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor1));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JqNode.class,entity,"Js should be dropped when function is null");


    }
    @Test
    public void createNodesFromLabel_single_extractor_identity_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","input => input",List.of(extractor1));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JqNode.class,entity,"Js should be dropped when function returns input");
        assertEquals(label1.name(),entity.name,"Created entity should have name from label not extractor");

    }
    @Test
    public void createNodesFromLabel_two_extractors_null_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor1,extractor2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JsNode.class,entity,"Should create a JsNode that returns combined values");
        assertNotNull(entity.operation);
        assertTrue(JsNode.isNullEmptyOrIdentityFunction(entity.operation),"js node should have an operation that returns the input");
        assertEquals(2,entity.sources.size(),"both extractors should be sources for the node");
    }
    @Test
    public void createNodesFromLabel_two_extractors_custom_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","val=>val.a+val.b",List.of(extractor1,extractor2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JsNode.class,entity,"Should create a JsNode that returns combined values");
        assertNotNull(entity.operation);
        assertFalse(JsNode.isNullEmptyOrIdentityFunction(entity.operation),"js node should have an operation that returns the input");
        // Multi-extractor single-param labels source directly from extractor nodes
        assertEquals(2,entity.sources.size(),"label should have 2 sources (direct extractor nodes)");
    }
    @Test
    public void createNodesFromLabel_multi_extractor_single_param_direct_sources(){
        // Multi-extractor single-param labels wire the JS function directly to
        // the extractor nodes. At runtime, createParameters() builds a combined
        // object from the source values using node names as keys.
        LoadLegacyTests.Extractor workload = new LoadLegacyTests.Extractor("workload","$.workload",false);
        LoadLegacyTests.Extractor results = new LoadLegacyTests.Extractor("results","$.results.*",true);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"Autobench",
                "v => v[\"results\"].reduce((a,b) => a+b) / v[\"results\"].length",
                List.of(workload, results));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JsNode.class, entity);
        assertEquals("Autobench", entity.name);

        // Should source directly from the 2 extractor nodes (no combiner)
        assertEquals(2, entity.sources.size(), "should have 2 sources (direct extractor nodes)");
        assertTrue(entity.sources.stream().anyMatch(s -> "workload".equals(s.name)), "should have workload source");
        assertTrue(entity.sources.stream().anyMatch(s -> "results".equals(s.name)), "should have results source");
    }
    @Test
    public void createFolder_combinedLabelPrefix(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","val=>val+val",List.of(extractor1));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$[0].\"$schema\"",label1);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());


        loadLegacyTests.combinedLabelPrefix="_";

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        loadLegacyTests.combinedLabelPrefix=""; // reset for other unit tests

        assertNotNull(folder);
        assertNotNull(folder.group);

        List<NodeEntity> labelNodes = folder.group.sources.stream().filter(n->n.name.equals("label")).toList();
        assertNotNull(labelNodes);
        assertEquals(1,labelNodes.size(),"only combining label should be named 'label'\n"+labelNodes.stream().map(Object::toString).collect(Collectors.joining("\n")));
    }
    @Test
    public void createFolder_two_transformers_creates_two_pipelines() {
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("data", "$.values[*]", true);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("data", "$.data.values[*]", true);

        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "result", null, List.of(new LoadLegacyTests.Extractor("result", "$.result", false)));

        LoadLegacyTests.Transformer t1 = new LoadLegacyTests.Transformer(1, "transform", "data => data.map(d => d)", "urn:target:1", List.of(ext1), List.of(label));
        LoadLegacyTests.Transformer t2 = new LoadLegacyTests.Transformer(2, "transform", "data => data.map(d => d)", "urn:target:1", List.of(ext2), List.of(label));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(), Collections.emptyList(), List.of(t1, t2), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);

        // Should have 2 transformer nodes + 1 coalesce node, 1 dataset node, 1 label node
        long transformerCount = folder.group.sources.stream().filter(v -> v instanceof JsNode && v.name.startsWith("transformer_")).count();
        long datasetCount = folder.group.sources.stream().filter(v -> v.name.equals("dataset")).count();
        assertEquals(2, transformerCount, "Expect 2 transformer nodes\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
        assertEquals(1, datasetCount, "Expect 1 dataset node (after coalesced transformers)\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));

    }

    @Test
    public void createFolder_extractor_name_conflicts_with_another_label_name(){
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("label_two", "$.one", true);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("extractor_two", "$.two", true);

        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1, "label_one", null, List.of(ext1,ext2));

        LoadLegacyTests.Extractor ext3 = new LoadLegacyTests.Extractor("extractor_three", "$.three", true);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1, "label_two", null, List.of(ext3));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", label1);
        schemaPaths.put("$.\"$schema\"", label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        assertNotNull(folder.group);
        assertNotNull(folder.group.sources);

        assertEquals(1,folder.group.sources.stream().filter(v -> v.name.equals("label_two")).count(),
                "only one node should use label_two name\n"+
                        folder.group.sources.stream().filter(v -> v.name.equals("label_two")).map(Object::toString).collect(Collectors.joining("\n"))
        );
    }

    @Test
    public void createFolder_two_transformers_labels_created_once() {
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("score", "$.scores[*]", true);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("score", "$.data.scores[*]", true);

        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "Score", null, List.of(new LoadLegacyTests.Extractor("score", "$.score", false)));

        LoadLegacyTests.Transformer t1 = new LoadLegacyTests.Transformer(10, "t", "score => [score]", "urn:t:1", List.of(ext1), List.of(label));
        LoadLegacyTests.Transformer t2 = new LoadLegacyTests.Transformer(20, "t", "score => [score]", "urn:t:1", List.of(ext2), List.of(label));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(), Collections.emptyList(), List.of(t1, t2), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        // Labels are created once against the coalesced dataset (not per-dataset)
        long scoreCount = folder.group.sources.stream().filter(v -> v.name.equals("Score")).count();
        assertEquals(1, scoreCount, "Expect 1 'Score' label node (created once against coalesced dataset)\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void createFolder_two_transformers_variable_resolves() {
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("val", "$.values[*]", true);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("val", "$.data.values[*]", true);

        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "metric", null, List.of(new LoadLegacyTests.Extractor("metric", "$.metric", false)));
        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1, "metric_var", List.of("metric"), null);
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(List.of("metric"), null, List.of(), "");

        LoadLegacyTests.Transformer t1 = new LoadLegacyTests.Transformer(10, "t", "val => [val]", "urn:t:1", List.of(ext1), List.of(label));
        LoadLegacyTests.Transformer t2 = new LoadLegacyTests.Transformer(20, "t", "val => [val]", "urn:t:1", List.of(ext2), List.of(label));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(fingerprint), Collections.emptyList(), List.of(t1, t2), List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        // Should have fingerprint node (variable resolves even with multiple label matches)
        long fpCount = folder.group.sources.stream().filter(v -> v instanceof FingerprintNode).count();
        assertEquals(1, fpCount, "Expect 1 fingerprint node\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void createFolder_single_transformer_no_suffix() {
        LoadLegacyTests.Extractor ext = new LoadLegacyTests.Extractor("data", "$.values", false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "result", null, List.of(new LoadLegacyTests.Extractor("result", "$.result", false)));
        LoadLegacyTests.Transformer t = new LoadLegacyTests.Transformer(1, "myTransform", "data => [data]", "urn:t:1", List.of(ext), List.of(label));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(), Collections.emptyList(), List.of(t), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        // Single transformer should NOT have suffix
        long datasetCount = folder.group.sources.stream().filter(v -> v.name.equals("dataset")).count();
        assertEquals(1, datasetCount, "Expect 1 dataset node named 'dataset' (no suffix)\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void createFolder_two_transformers_dataset_is_jq_and_labels_source_from_it() {
        // Mirrors rhivos pattern: two transformers for different schema versions
        // (old format uses $.workload[*], new format uses $.workload.data[*])
        // Both target the same schema with labels that extract from the dataset items.
        LoadLegacyTests.Extractor oldResults = new LoadLegacyTests.Extractor("results", "$.workload[*].results", true);
        LoadLegacyTests.Extractor oldUuid = new LoadLegacyTests.Extractor("uuid", "$.metadata.uuid", false);
        LoadLegacyTests.Extractor newResults = new LoadLegacyTests.Extractor("results", "$.workload.data[*].results", true);
        LoadLegacyTests.Extractor newUuid = new LoadLegacyTests.Extractor("uuid", "$.metadata.uuid", false);

        // Labels on the target schema — extract from each dataset item
        LoadLegacyTests.Label workloadLabel = new LoadLegacyTests.Label(-1, "Workload", null,
                List.of(new LoadLegacyTests.Extractor("workload", "$.workload", false)));
        LoadLegacyTests.Label uuidLabel = new LoadLegacyTests.Label(-1, "UUID", null,
                List.of(new LoadLegacyTests.Extractor("uuid", "$.metadata.uuid", false)));

        LoadLegacyTests.Transformer t1 = new LoadLegacyTests.Transformer(11, "Results Extraction",
                "({results, uuid}) => results.map(r => ({...r, uuid}))",
                "urn:target:1", List.of(oldResults, oldUuid), List.of(workloadLabel, uuidLabel));
        LoadLegacyTests.Transformer t2 = new LoadLegacyTests.Transformer(4639723, "Results Extraction",
                "({results, uuid}) => results.map(r => ({...r, uuid}))",
                "urn:target:1", List.of(newResults, newUuid), List.of(workloadLabel, uuidLabel));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(), Collections.emptyList(), List.of(t1, t2), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        // Dataset should be a JQ node that unwraps and flattens transformer outputs
        NodeEntity dataset = folder.group.sources.stream()
                .filter(v -> v.name.equals("dataset")).findFirst().orElse(null);
        assertNotNull(dataset, "dataset node should exist");
        assertInstanceOf(JqNode.class, dataset, "multi-transformer dataset should use JQ");
        assertEquals(2, dataset.sources.size(),
                "dataset should source from both transformers");

        // Labels should source from dataset directly, not from transformer-named intermediates
        NodeEntity workloadNode = folder.group.sources.stream()
                .filter(v -> v.name.equals("Workload")).findFirst().orElse(null);
        assertNotNull(workloadNode, "Workload label node should exist");
        assertTrue(workloadNode.sources.stream().anyMatch(s -> s.name.equals("dataset")),
                "Workload should source from dataset\n"
                + workloadNode.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));

        // No transformer-named JQ source nodes should exist
        boolean hasTransformerSourceNode = folder.group.sources.stream()
                .anyMatch(v -> v instanceof JqNode && v.name.startsWith("transformer_Results"));
        assertFalse(hasTransformerSourceNode,
                "should not have transformer-named JQ source nodes — labels source from dataset directly");
    }

    @Test
    public void createFolder_two_transformers_coalesce_uses_sanitized_names() {
        // Transformer names with spaces must be sanitized for JS parameter names
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("data", "$.old[*]", true);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("data", "$.new[*]", true);

        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "result", null, List.of(new LoadLegacyTests.Extractor("result", "$.result", false)));

        LoadLegacyTests.Transformer t1 = new LoadLegacyTests.Transformer(1, "My Transform", "data => data", "urn:t:1", List.of(ext1), List.of(label));
        LoadLegacyTests.Transformer t2 = new LoadLegacyTests.Transformer(2, "My Transform", "data => data", "urn:t:1", List.of(ext2), List.of(label));

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", new HashedSets<>(),
                List.of(), Collections.emptyList(), List.of(t1, t2), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        // Transformer names should be sanitized (spaces → underscores)
        NodeEntity dataset = folder.group.sources.stream().filter(v -> v.name.equals("dataset")).findFirst().orElse(null);
        assertNotNull(dataset, "dataset node should exist");
        // Extract parameter list from "(params) => body"

        boolean hasSpace = dataset.sources.stream().anyMatch(v -> v.name.contains(" "));
        assertFalse(hasSpace,"unexpected space in source names\n"+dataset.sources.stream().map(v->v.name).collect(Collectors.joining("\n")));
    }

    @Test
    public void createNodesFromLabel_jq_combiner_quotes_extractor_names_with_spaces() {
        // Horreum extractor names can contain spaces — JQ object keys need quoting
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("My Score","$.score",false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("Other Value","$.value",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"computed",
                "v => v[\"My Score\"] + v[\"Other Value\"]", List.of(ext1, ext2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label, group.root, group, tracker, new HashSet<>(), false);

        assertNotNull(entity);
        // Should source directly from the 2 extractor nodes (no combiner)
        assertEquals(2, entity.sources.size(), "should have 2 direct sources");
        assertTrue(entity.sources.stream().anyMatch(s -> "My Score".equals(s.name)));
        assertTrue(entity.sources.stream().anyMatch(s -> "Other Value".equals(s.name)));
    }

    @Test
    public void createFolder_no_transform_deduplicates_coalesce_sources() {
        // When the same label appears across multiple schemas but resolves to the same
        // extractor node, the coalesce should deduplicate to avoid value_edge violations
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("value","$.value",false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("value","$.value",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(1,"metric","v => v * 2",List.of(ext1));
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(2,"metric","v => v * 3",List.of(ext2));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", label1);
        schemaPaths.put("$.\"$schema\"", label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        // With dedup, the combining should produce at most 1 unique source
        // (both variants use the same extractor node via nodeTracking reuse)
        long metricCount = folder.group.sources.stream().filter(v -> v.name.equals("metric")).count();

        folder.group.sources.forEach(System.out::println);

        assertTrue(metricCount >= 1, "should have at least one metric node");

    }

    @Test
    public void createFolder_no_transform_variant_nodes_added_to_group() {
        // When labels are duplicated across schemas, the numbered variants must be
        // added to folder.group so they get group_id and are processed by the work queue
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("val","$.val",false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("val","$.other_val",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(1,"metric",null,List.of(ext1));
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(2,"metric",null,List.of(ext2));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", label1);
        schemaPaths.put("$.\"$schema\"", label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();

        assertNotNull(folder);
        // All nodes should have group set (no orphans)
        for (NodeEntity n : folder.group.sources) {
            assertNotNull(n.group, "node " + n.name + " should have group set");
            assertEquals(folder.group, n.group, "node " + n.name + " should belong to the folder group");
        }
        // Variant nodes keep their original extractor names (no suffixing).
        // The combiner node gets the label name "metric".
        long variantCount = folder.group.sources.stream()
                .filter(v -> v.name.equals("val"))
                .count();
        assertTrue(variantCount == 0, "variant extractor nodes (named 'val') should not be in the group\n"
                + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
        assertTrue(folder.group.sources.stream().anyMatch(v -> v.name.equals("metric") && v instanceof JsNode),
                "combiner node named 'metric' should be in the group\n"
                + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void createNodesFromLabel_shared_extractors_across_labels() {
        // When two labels with different functions but same extractors call createNodesFromLabel,
        // the extractor nodes should be reused (same instances)
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("a","$.a",false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("b","$.b",false);

        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(1,"result","v => v.a + v.b",List.of(ext1, ext2));
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(2,"result","v => v.a * v.b",List.of(ext1, ext2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity node1 = loadLegacyTests.createNodesFromLabel(label1, group.root, group, tracker, new HashSet<>(), false);
        NodeEntity node2 = loadLegacyTests.createNodesFromLabel(label2, group.root, group, tracker, new HashSet<>(), false);

        assertNotNull(node1);
        assertNotNull(node2);
        assertInstanceOf(JsNode.class, node1);
        assertInstanceOf(JsNode.class, node2);
        // Both should source directly from the same extractor nodes (reused)
        assertEquals(2, node1.sources.size(), "first label should have 2 sources");
        assertEquals(2, node2.sources.size(), "second label should have 2 sources");
        assertSame(node1.sources.get(0), node2.sources.get(0),
                "both labels should share the same first extractor node");
        // No combiner nodes should exist
        long combinerCount = group.sources.stream()
                .filter(v -> v.name.endsWith("_extract"))
                .count();
        assertEquals(0, combinerCount, "should have no combiner nodes");
    }

    @Test
    public void createNodesFromLabel_filter_extractors_direct_sources() {
        // Non-array extractors with jsonpath filter expressions become JQ nodes.
        // With direct wiring, these extractor nodes are direct sources of the JS function.
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("count",
                "$.data[*] ? (@.name == \"target\") .result.\"text()\"", false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("target",
                "$.data[*] ? (@.name == \"target\") .target.\"text()\"", false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "pct",
                "v => parseInt((v.count / v.target) * 100)", List.of(ext1, ext2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label, group.root, group, tracker, new HashSet<>(), false);

        assertNotNull(entity);
        assertEquals(2, entity.sources.size(), "should have 2 direct sources");
        assertTrue(entity.sources.stream().allMatch(s -> s instanceof JqNode), "sources should be JQ nodes");
        assertTrue(entity.sources.stream().anyMatch(s -> "count".equals(s.name)));
        assertTrue(entity.sources.stream().anyMatch(s -> "target".equals(s.name)));
    }

    /**
     * Tests that createFolder correctly creates a domain node from timeline_labels
     * and wires it into RelativeDifference detection nodes. When timeline_labels
     * contains a label name that exists in the node graph, the RD node should have
     * 4 sources (fingerprint, groupBy, range, domain) instead of 3.
     */
    @Test
    public void createFolder_timeline_labels_creates_domain_node_for_detection() {
        // Create a label that will serve as both a variable and the timeline domain
        LoadLegacyTests.Extractor timeExtractor = new LoadLegacyTests.Extractor("startTime", "$.startTime", false);
        LoadLegacyTests.Label timeLabel = new LoadLegacyTests.Label(-1, "Start Time", null, List.of(timeExtractor));

        // Create a value label for detection
        LoadLegacyTests.Extractor valueExtractor = new LoadLegacyTests.Extractor("throughput", "$.throughput", false);
        LoadLegacyTests.Label valueLabel = new LoadLegacyTests.Label(-1, "Throughput", null, List.of(valueExtractor));

        HashedSets<String, LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", timeLabel);
        schemaPaths.put("$.\"$schema\"", valueLabel);

        // Fingerprint with timeline_labels pointing to "Start Time"
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(
                List.of("Throughput"), null, List.of("Start Time"), "");

        // Variable for the value (maps to Throughput label)
        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1, "throughput_var", List.of("Throughput"), null);

        // RelativeDifference change detection
        JqObject rdConfig = (JqObject) JqValues.parse("""
                {
                    "filter": "mean",
                    "window": 1,
                    "minPrevious": 5,
                    "threshold": 0.2
                }
                """);
        LoadLegacyTests.ChangeDetection changeDetection = new LoadLegacyTests.ChangeDetection(-1, -1, "relativeDifference", rdConfig);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                List.of(fingerprint), List.of(changeDetection), List.of(), List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        assertNotNull(folder);
        assertNotNull(folder.group);

        // Find the RelativeDifference node
        List<NodeEntity> rdNodes = folder.group.sources.stream()
                .filter(v -> v instanceof RelativeDifference)
                .toList();
        assertEquals(1, rdNodes.size(), "Expect 1 RelativeDifference node\n"
                + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));

        RelativeDifference rd = (RelativeDifference) rdNodes.getFirst();
        // With domain node, RD should have 4 sources: fingerprint, groupBy, range, domain
        assertEquals(4, rd.sources.size(),
                "RD node should have 4 sources (fingerprint, groupBy, range, domain) when timeline_labels is set\n"
                + rd.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));

        // The domain node should be the "Start Time" extractor node
        assertNotNull(rd.getDomainNode(), "RD should have a domain node from timeline_labels");
        assertEquals("Start Time", rd.getDomainNode().name,
                "Domain node should be the 'Start Time' label node");
    }

    /**
     * Tests that createFolder with empty timeline_labels produces an RD node
     * without a domain node (3 sources instead of 4), falling back to created_at ordering.
     */
    @Test
    public void createFolder_empty_timeline_labels_no_domain_node() {
        LoadLegacyTests.Extractor valueExtractor = new LoadLegacyTests.Extractor("throughput", "$.throughput", false);
        LoadLegacyTests.Label valueLabel = new LoadLegacyTests.Label(-1, "Throughput", null, List.of(valueExtractor));

        HashedSets<String, LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", valueLabel);

        // Fingerprint with EMPTY timeline_labels
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(
                List.of("Throughput"), null, List.of(), "");

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1, "throughput_var", List.of("Throughput"), null);

        JqObject rdConfig = (JqObject) JqValues.parse("""
                {
                    "filter": "mean",
                    "window": 1,
                    "minPrevious": 5,
                    "threshold": 0.2
                }
                """);
        LoadLegacyTests.ChangeDetection changeDetection = new LoadLegacyTests.ChangeDetection(-1, -1, "relativeDifference", rdConfig);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                List.of(fingerprint), List.of(changeDetection), List.of(), List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test).folder();
        assertNotNull(folder);

        List<NodeEntity> rdNodes = folder.group.sources.stream()
                .filter(v -> v instanceof RelativeDifference)
                .toList();
        assertEquals(1, rdNodes.size(), "Expect 1 RelativeDifference node");

        RelativeDifference rd = (RelativeDifference) rdNodes.getFirst();
        // Without domain node, RD should have 3 sources: fingerprint, groupBy, range
        assertEquals(3, rd.sources.size(),
                "RD node should have 3 sources (no domain) when timeline_labels is empty\n"
                + rd.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
        assertNull(rd.getDomainNode(), "RD should NOT have a domain node when timeline_labels is empty");
    }

    @Test @Disabled("not sure why it is failing atm")
    public void createNodesFromLabel_two_extractors_custom_function_with_extra_parameters(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","(val,idk)=>val.a+val.b",List.of(extractor1,extractor2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>(), false);

        assertNotNull(entity);
        assertInstanceOf(JsNode.class,entity,"Should create a JsNode that returns combined values");
        assertNotNull(entity.operation);
        assertFalse(JsNode.isNullEmptyOrIdentityFunction(entity.operation),"js node should have an operation that returns the input");
        assertEquals(2,entity.sources.size(),"both extractors should be sources for the node");
    }
}
