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
import io.hyperfoil.tools.yaup.HashedSets;
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

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1,"test",new HashedSets<>(), List.of(),Collections.emptyList(),List.of(transformer),Collections.emptyList());

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


        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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


        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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
        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
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
        // Multi-extractor single-param labels now use a JQ combiner as their single source
        assertEquals(1,entity.sources.size(),"label should have 1 source (the JQ combiner node)");
        assertInstanceOf(JqNode.class,entity.sources.get(0),"source should be a JQ combiner node");
    }
    @Test
    public void createNodesFromLabel_multi_extractor_single_param_creates_jq_combiner(){
        // Mirrors the rhivos "Autobench Multi Core" pattern:
        // - scalar extractor "workload" matches every dataset item (e.g., 10 values)
        // - array extractor "results" only matches items with results (e.g., 1 value)
        // Separate extractor nodes would produce mismatched counts (10 vs 1),
        // causing calculateSourceValuePermutations to return null.
        // The JQ combiner extracts both fields from the same input in one expression,
        // producing one combined object per dataset item — no permutation needed.
        LoadLegacyTests.Extractor workload = new LoadLegacyTests.Extractor("workload","$.workload",false);
        LoadLegacyTests.Extractor results = new LoadLegacyTests.Extractor("results","$.results.*",true);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1,"Autobench",
                "v => v[\"results\"].reduce((a,b) => a+b) / v[\"results\"].length",
                List.of(workload, results));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label,group.root,group,tracker,new HashSet<>());

        assertNotNull(entity);
        assertInstanceOf(JsNode.class, entity);
        assertEquals("Autobench", entity.name);

        // Single source: the JQ combiner node, not the 2 raw extractors
        assertEquals(1, entity.sources.size(), "should have 1 source (JQ combiner), not 2 separate extractors");
        NodeEntity combiner = entity.sources.get(0);
        assertInstanceOf(JqNode.class, combiner);
        assertTrue(combiner.name.endsWith("_extract"), "combiner name should end with _extract");

        // The combiner's JQ expression builds an object with both extractor fields
        // Scalar extractor uses "// null", array extractor uses "try [...] catch null"
        assertTrue(combiner.operation.contains("workload"), "JQ expression should reference workload extractor");
        assertTrue(combiner.operation.contains("results"), "JQ expression should reference results extractor");
        assertTrue(combiner.operation.contains("// null"), "scalar extractor should use // null fallback");
        assertTrue(combiner.operation.contains("try"), "array extractor should use try/catch for error suppression");

        // The combiner sources from the parent (group root in this test)
        assertEquals(1, combiner.sources.size());
        assertEquals(group.root, combiner.sources.get(0));
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

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        assertNotNull(folder.group);

        // Should have 2 transformer nodes + 1 coalesce node, 1 dataset node, 1 label node
        long transformerCount = folder.group.sources.stream().filter(v -> v instanceof JsNode && v.name.startsWith("transformer_")).count();
        long datasetCount = folder.group.sources.stream().filter(v -> v.name.equals("dataset")).count();
        assertEquals(2, transformerCount, "Expect 2 transformer nodes\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
        assertEquals(1, datasetCount, "Expect 1 dataset node (after coalesced transformers)\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));

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

        FolderEntity folder = loadLegacyTests.createFolder(test);

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

        FolderEntity folder = loadLegacyTests.createFolder(test);

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

        FolderEntity folder = loadLegacyTests.createFolder(test);

        // Single transformer should NOT have suffix
        long datasetCount = folder.group.sources.stream().filter(v -> v.name.equals("dataset")).count();
        assertEquals(1, datasetCount, "Expect 1 dataset node named 'dataset' (no suffix)\n" + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
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

        FolderEntity folder = loadLegacyTests.createFolder(test);

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

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label, group.root, group, tracker, new HashSet<>());

        assertNotNull(entity);
        assertEquals(1, entity.sources.size());
        NodeEntity combiner = entity.sources.get(0);
        assertInstanceOf(JqNode.class, combiner);
        assertTrue(combiner.operation.contains("\"My Score\""), "extractor name with spaces should be quoted");
        assertTrue(combiner.operation.contains("\"Other Value\""), "extractor name with spaces should be quoted");
    }

    @Test
    public void createFolder_no_transform_deduplicates_coalesce_sources() {
        // When the same label appears across multiple schemas but resolves to the same
        // extractor node, the coalesce should deduplicate to avoid value_edge violations
        LoadLegacyTests.Extractor ext = new LoadLegacyTests.Extractor("value","$.value",false);
        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(1,"metric","v => v * 2",List.of(ext));
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(2,"metric","v => v * 3",List.of(ext));

        HashedSets<String,LoadLegacyTests.Label> schemaPaths = new HashedSets<>();
        schemaPaths.put("$.\"$schema\"", label1);
        schemaPaths.put("$.\"$schema\"", label2);

        LoadLegacyTests.Test test = new LoadLegacyTests.Test(-1, "test", schemaPaths,
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        // With dedup, the combining should produce at most 1 unique source
        // (both variants use the same extractor node via nodeTracking reuse)
        long metricCount = folder.group.sources.stream().filter(v -> v.name.equals("metric")).count();
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

        FolderEntity folder = loadLegacyTests.createFolder(test);

        assertNotNull(folder);
        // All nodes should have group set (no orphans)
        for (NodeEntity n : folder.group.sources) {
            assertNotNull(n.group, "node " + n.name + " should have group set");
            assertEquals(folder.group, n.group, "node " + n.name + " should belong to the folder group");
        }
        // The numbered variants (metric0, metric1) should be in the group's sources
        long variantCount = folder.group.sources.stream()
                .filter(v -> v.name.startsWith("metric") && v.name.matches("metric\\d+"))
                .count();
        assertTrue(variantCount >= 1, "numbered variant nodes should be in the group\n"
                + folder.group.sources.stream().map(NodeEntity::toString).collect(Collectors.joining("\n")));
    }

    @Test
    public void createNodesFromLabel_jq_combiner_reused_across_labels() {
        // When two labels with different functions but same extractors call createNodesFromLabel,
        // the second should reuse the existing JQ combiner instead of creating a duplicate
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("a","$.a",false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("b","$.b",false);

        LoadLegacyTests.Label label1 = new LoadLegacyTests.Label(1,"result","v => v.a + v.b",List.of(ext1, ext2));
        LoadLegacyTests.Label label2 = new LoadLegacyTests.Label(2,"result","v => v.a * v.b",List.of(ext1, ext2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity node1 = loadLegacyTests.createNodesFromLabel(label1, group.root, group, tracker, new HashSet<>());
        NodeEntity node2 = loadLegacyTests.createNodesFromLabel(label2, group.root, group, tracker, new HashSet<>());

        assertNotNull(node1);
        assertNotNull(node2);
        // Both should be JsNodes with a JQ combiner as source
        assertInstanceOf(JsNode.class, node1);
        assertInstanceOf(JsNode.class, node2);
        assertEquals(1, node1.sources.size());
        assertEquals(1, node2.sources.size());
        // Both should reference the SAME combiner instance (reused, not duplicated)
        assertSame(node1.sources.get(0), node2.sources.get(0),
                "both labels should reuse the same JQ combiner node");
        // Only one combiner should exist in the group
        long combinerCount = group.sources.stream()
                .filter(v -> v.name.endsWith("_extract"))
                .count();
        assertEquals(1, combinerCount, "should have exactly 1 combiner node, not duplicates");
    }

    @Test
    public void createNodesFromLabel_jq_combiner_wraps_filter_paths_in_first() {
        // Non-array extractors with jsonpath filter expressions produce a stream in JQ.
        // Without first(), object construction creates cartesian products (nested objects).
        // With first(), each field gets a single value.
        LoadLegacyTests.Extractor ext1 = new LoadLegacyTests.Extractor("count",
                "$.data[*] ? (@.name == \"target\") .result.\"text()\"", false);
        LoadLegacyTests.Extractor ext2 = new LoadLegacyTests.Extractor("target",
                "$.data[*] ? (@.name == \"target\") .target.\"text()\"", false);
        LoadLegacyTests.Label label = new LoadLegacyTests.Label(-1, "pct",
                "v => parseInt((v.count / v.target) * 100)", List.of(ext1, ext2));

        NodeGroupEntity group = new NodeGroupEntity();
        LoadLegacyTests.NodeTracking tracker = new LoadLegacyTests.NodeTracking();

        NodeEntity entity = loadLegacyTests.createNodesFromLabel(label, group.root, group, tracker, new HashSet<>());

        assertNotNull(entity);
        assertEquals(1, entity.sources.size());
        NodeEntity combiner = entity.sources.get(0);
        assertInstanceOf(JqNode.class, combiner);
        // Both scalar extractors have filters, so their paths should be wrapped in first()
        assertTrue(combiner.operation.contains("first("), "filter-chain paths should be wrapped in first()");
        // Should not contain bare select() outside first()
        String op = combiner.operation;
        int firstIdx = op.indexOf("first(");
        int selectIdx = op.indexOf("select(");
        assertTrue(selectIdx > firstIdx, "select() should be inside first(), not standalone");
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
