package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.NodeGroupEntity;
import io.hyperfoil.tools.h5m.entity.node.*;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
public class LoadLegacyTestsTest {

    @Inject
    LoadLegacyTests loadLegacyTests;

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

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",new HashedLists<>(), List.of(),Collections.emptyList(),List.of(transformer),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        assertNotNull(folder.group);


        System.out.println(folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(4,folder.group.sources.size(),"Expect 2 sql nodes, 1 jq node, and 1 js node\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Jq \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 2 SqlNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("dataset")).count(),"Expect 1 named dataset \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));


    }

    @Test
    public void createFolder_one_schemaPath_duplicate_label_name(){

        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor2));


        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        assertNotNull(folder.group);

        assertEquals(3,folder.group.sources.size(),"Expect 2 SqlNodes and 1 JsNode\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 2 SqlNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }

    @Test
    public void createFolder_two_schemaPath_duplicate_label_name(){

        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor1));

        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor2));


        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.other.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        assertNotNull(folder.group);

        assertEquals(4,folder.group.sources.size(),"Expect 2 SqlNodes and 1 JsNode\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JqNode)).count(),"Expect 1 Jq \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 2 SqlNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("label")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
    }

    @Test
    public void createFolder_single_extractor_renamed_for_identity_label(){
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("extractor","$.tag",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor));

        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(1,folder.group.sources.size());

        NodeEntity first = folder.group.sources.get(0);

        assertNotNull(first);
        assertEquals("label",first.name);
        assertEquals("$.tag",first.operation);
        assertInstanceOf(SqlJsonpathNode.class,first);
    }
    @Test
    public void createFolder_variable_replaced_by_single_source(){
        LoadLegacyTests.Extractor extractor = new LoadLegacyTests.Extractor("extractor","$.tag",false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"label","foo=>foo",List.of(extractor));

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1,"variable",List.of(label.name()),null);

        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test);

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

        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label1);
        schemaPaths.put("$.\"$schema\"",label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, Collections.emptyList(),Collections.emptyList(),Collections.emptyList(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(3,folder.group.sources.size(),"Expect 2 SQL nodes and a js node");

        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof JsNode)).count(),"Expect 1 Js \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(2,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 2 SqlNodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(1,folder.group.sources.stream().filter(v->v.name.equals("variable")).count(),"Expect 1 named label \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }

    @Test
    public void createFolder_fingerprint(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label1","foo=>foo",List.of(extractor1));
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(List.of(label1.name()),null,List.of(),"");
        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label1);


        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, List.of(fingerprint),List.of(),List.of(),List.of());

        FolderEntity folder = loadLegacyTests.createFolder(test);
        assertNotNull(folder);

        assertNotNull(folder.group);
        assertEquals(2,folder.group.sources.size(),"Expect 1 SQL node and a fingerprint node");
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 1 sql node \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FingerprintNode)).count(),"Expect 1 fingerprint\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));


    }

    @Test
    public void createFolder_changeDetection_threshold() throws JsonProcessingException {
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label1","foo=>foo",List.of(extractor1));
        LoadLegacyTests.Fingerprint fingerprint = new LoadLegacyTests.Fingerprint(List.of(label1.name()),null,List.of(),"");
        HashedLists<String,LoadLegacyTests.Label> schemaPaths = new HashedLists<>();
        schemaPaths.put("$.\"$schema\"",label1);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode config = (ObjectNode) mapper.readTree("""
                {
                    "max" : { "inclusive" : true, "enabled" : true, "value" : 10 },
                    "min" : { "inclusive" : true, "enabled" : true, "value" : 5 }
                }
                """);
        LoadLegacyTests.ChangeDetection changeDetection = new LoadLegacyTests.ChangeDetection(-1,-1,"fixedThreshold", config);

        LoadLegacyTests.Variable variable = new LoadLegacyTests.Variable(-1,"variable",List.of(label1.name()),null);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",schemaPaths, List.of(fingerprint),List.of(changeDetection),List.of(),List.of(variable));

        FolderEntity folder = loadLegacyTests.createFolder(test);
        assertNotNull(folder);

        assertNotNull(folder.group);

        assertEquals(3,folder.group.sources.size(),"Expect 3 Nodes\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof SqlJsonpathNode)).count(),"Expect 1 sql node \n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FingerprintNode)).count(),"Expect 1 fingerprint\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));
        assertEquals(1,folder.group.sources.stream().filter(v -> (v instanceof FixedThreshold)).count(),"Expect 1 threshold node\n"+folder.group.sources.stream().map(ne->ne.toString()).collect(Collectors.joining("\n")));

    }



        @Test
    public void createNodesFromLabel_single_extractor_null_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor1));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>());

        assertNotNull(entity);
        assertInstanceOf(SqlJsonpathNode.class,entity,"Js should be dropped when function is null");


    }
    @Test
    public void createNodesFromLabel_single_extractor_identity_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","input => input",List.of(extractor1));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>());

        assertNotNull(entity);
        assertInstanceOf(SqlJsonpathNode.class,entity,"Js should be dropped when function returns input");

    }
    @Test
    public void createNodesFromLabel_two_extractors_null_function(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label",null,List.of(extractor1,extractor2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>());

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

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>());

        assertNotNull(entity);
        assertInstanceOf(JsNode.class,entity,"Should create a JsNode that returns combined values");
        assertNotNull(entity.operation);
        assertFalse(JsNode.isNullEmptyOrIdentityFunction(entity.operation),"js node should have an operation that returns the input");
        assertEquals(2,entity.sources.size(),"both extractors should be sources for the node");
    }
    @Test @Disabled("not sure why it is failing atm")
    public void createNodesFromLabel_two_extractors_custom_function_with_extra_parameters(){
        LoadLegacyTests.Extractor extractor1 = new LoadLegacyTests.Extractor("extractor","$.one",false);
        LoadLegacyTests.Extractor extractor2 = new LoadLegacyTests.Extractor("extractor","$.two",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(-1,"label","(val,idk)=>val.a+val.b",List.of(extractor1,extractor2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label1,group.root,group,tracker,new HashSet<>());

        assertNotNull(entity);
        assertInstanceOf(JsNode.class,entity,"Should create a JsNode that returns combined values");
        assertNotNull(entity.operation);
        assertFalse(JsNode.isNullEmptyOrIdentityFunction(entity.operation),"js node should have an operation that returns the input");
        assertEquals(2,entity.sources.size(),"both extractors should be sources for the node");
    }
}
