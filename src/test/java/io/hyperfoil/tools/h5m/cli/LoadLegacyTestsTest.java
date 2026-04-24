package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.api.Node;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.entity.NodeEntity;
import io.hyperfoil.tools.h5m.entity.node.JsNode;
import io.hyperfoil.tools.h5m.entity.node.SqlJsonpathNode;
import io.hyperfoil.tools.h5m.svc.FolderService;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.yaup.HashedLists;
import io.hyperfoil.tools.yaup.HashedSets;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
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


        System.out.println("folder="+folder);
    }
}
