package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.provided.DatasourceConfiguration;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusMainTest
public class H5mTest {
    public static List<LaunchResult> run(QuarkusMainLauncher launcher,String[]... args){
        return Arrays.stream(args).map(arg->{
            System.out.println("run: "+Arrays.toString(arg));
            return launcher.launch(arg);
        }).toList();
    }
    @BeforeEach
    public void dropDb(){
        ///tmp/h5m-test.db-shm, /tmp/h5m-test.db-wal, /tmp/h5m-test.db
        String path = DatasourceConfiguration.getPath();
        List.of("","-shm","-wal").forEach(suffix->{
            File f = new File(path+suffix);
            if(f.exists()){
                f.delete();
            } else{ }
        });
    }
    @Test
    public void list(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("list");
        assertEquals(0,result.exitCode(),result.getOutput());
    }

    @Test
    public void help(QuarkusMainLauncher launcher) {
        LaunchResult result = launcher.launch("help");
        assertEquals(0,result.exitCode(),result.getOutput());
    }

    @Test
    public void add_folder(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"list","folders"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains(testName),result.getOutput());
    }

    @Test
    public void list_folder(QuarkusMainLauncher launcher) {
        List<LaunchResult> results = run(launcher,
            new String[]{"add","folder","foo"},
            new String[]{"add","folder","bar"}
        );
        results.forEach(result->{
           assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        for(List<String> command : List.of(List.of("list","folder"),List.of("list","folders"))){
            result = launcher.launch(command.toArray(new String[0]));
            assertEquals(0,result.exitCode(),result.getOutput());
            assertTrue(result.getOutput().contains("foo"),"expect to find foo folder:\n"+result.getOutput());
            assertTrue(result.getOutput().contains("bar"),"expect to find bar folder:\n"+result.getOutput());
        }
    }
    @Test
    public void remove_folder(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"remove","folder",testName},
                new String[]{"list","folders"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertFalse(result.getOutput().contains(testName),"expect to not find foo folder: "+result.getOutput());
    }
    @Test
    public void add_js_uses_other_nodes(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".buz"},
                new String[]{"add","jq","to",testName,"bar",".bar"},
                new String[]{"add","jq","to",testName,"biz",".biz"},
                new String[]{"add","js","to",testName,"dataset","function* dataset({foo, bar, biz}){\nyield foo;\nyield bar;\nyield biz;\n}"},
                new String[]{"list",testName,"nodes"},
                new String[]{"list","nodes","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

    }
    @Test
    public void add_jq_list_node(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"buz",".buz"},
                new String[]{"add","jq","to",testName,"bizzing","{buz}:.biz"},
                new String[]{"list",testName,"nodes"},
                new String[]{"list","nodes","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("biz"),"expect to find biz: "+result.getOutput());
    }
    @Test
    public void add_relativedifference_list_node(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"domainNode",".x"},
                new String[]{"add","jq","to",testName,"rangeNode",".y"},
                new String[]{"add","jq","to",testName,"fp1",".fp1"},
                new String[]{"add","jq","to",testName,"fp2",".fp2"},
                new String[]{"list",testName,"nodes"},
                new String[]{"add","relativedifference","rd1","to",testName,"range","rangeNode","domain","domainNode","fingerprint","fp1,fp2"},
                new String[]{"list",testName,"nodes"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("rd1"),"expect to find rd1: "+result.getOutput());



    }
    @Test
    public void calculate_relativedifference_node(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "x": 3, "y": 1.1, "fp1": "alpha"
                }
                """
        );
        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                    "x": 2, "y": 1.1, "fp1": "alpha"
                }
                """
        );
        Path filePath03 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                    "x": 1, "y": 2.1, "fp1": "alpha"
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"domainNode",".x"},
                new String[]{"add","jq","to",testName,"rangeNode",".y"},
                new String[]{"add","jq","to",testName,"fp1",".fp1"},
                new String[]{"list",testName,"nodes"},
                new String[]{"add","relativedifference","relativediff","to",testName,"range","rangeNode","domain","domainNode","fingerprint","fp1","window","1","minPrevious","1"},
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult last = results.getLast();
        assertTrue(last.getOutput().contains("Count: 13"),"expect 13 values from test");


    }
    @Test //not yet working because relativedifference doesn't know about the "datsaet" node
    //need to tell relativedifference which node is the dataset. either detect it with CTE (is that possible)
    //or make it an attribute on the Folder / NodeGroup
    public void calculate_relativedifference_dataset_node(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "each": [
                    {"x": 3, "y": 1.1, "fp1": "alpha", "fp2": "alpha"},
                    {"x": 13, "y": 20.2, "fp1": "alpha", "fp2": "bravo"}
                  ]
                }
                """
        );
        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "each": [
                    {"x": 2, "y": 2.1, "fp1": "alpha", "fp2": "alpha"},
                    {"x": 12, "y": 30.2, "fp1": "alpha", "fp2": "bravo"}
                  ]
                }
                """
        );
        Path filePath03 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "each": [
                    {"x": 1, "y": 3.1, "fp1": "alpha", "fp2": "alpha"},
                    {"x": 11, "y": 40.2, "fp1": "alpha", "fp2": "bravo"}
                  ]
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"split",".each[]"},
                new String[]{"add","jq","to",testName,"domainNode","{split}:.x"},
                new String[]{"add","jq","to",testName,"rangeNode","{split}:.y"},
                new String[]{"add","jq","to",testName,"fp1","{split}:.fp1"},
                new String[]{"add","jq","to",testName,"fp2","{split}:.fp2"},
                new String[]{"list",testName,"nodes"},
                new String[]{"add","relativedifference","relativediff","to",testName,"range","rangeNode","domain","domainNode","by","split","fingerprint","fp1,fp2","window","1","minPrevious","1"},
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult last = results.getLast();
        assertTrue(last.getOutput().contains("Count: 38"),"expect 38 values from test");
    }

    @Test
    public void remove_node(QuarkusMainLauncher launcher) {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"biz",".biz"},
                new String[]{"list",testName,"nodes"},
                new String[]{"remove","node","biz","from",testName},
                new String[]{"list",testName,"nodes"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertFalse(result.getOutput().contains("biz"),"expect to NOT find biz: "+result.getOutput());
    }

    @Test
    public void upload_list_values(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":{
                     "bar": {
                       "biz":"buz"
                     }
                  }
                }
                """
        );
        //filePath.toFile().deleteOnExit();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo"},
                new String[]{"add","jq","to",testName,"bar","{foo}:.bar"},
                new String[]{"add","jq","to",testName,"biz","{bar}:.biz"},
                new String[]{"list",testName,"nodes",},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 3"));
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"buz\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"buz\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" buz "),"result should contain .bar:" +result.getOutput());
    }
    @Test
    public void upload_folder_list_values(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":{
                     "bar": {
                       "biz":"buz"
                     }
                  }
                }
                """
        );
        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":{
                     "bar": {
                       "biz":"bur"
                     }
                  }
                }
                """
        );
        //filePath.toFile().deleteOnExit();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo"},
                new String[]{"add","jq","to",testName,"bar","{foo}:.bar"},
                new String[]{"add","jq","to",testName,"biz","{bar}:.biz"},
                new String[]{"list",testName,"nodes",},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 6"));
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"buz\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"bur\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"buz\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"bur\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" buz "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" bur "),"result should contain .bar:" +result.getOutput());
    }
    @Test
    public void upload_jsonata_list_values(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":{
                     "bar": {
                       "biz":"buz"
                     }
                  }
                }
                """
        );
        //filePath.toFile().deleteOnExit();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jsonata","to",testName,"foo","foo"},
                new String[]{"add","jsonata","to",testName,"bar","{foo}:bar"},
                new String[]{"add","jsonata","to",testName,"biz","{bar}:biz"},
                new String[]{"list",testName,"nodes",},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 3"),"expect 3 values\n"+result.getOutput());
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"buz\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"buz\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" buz "),"result should contain .bar:" +result.getOutput());
    }
    @Test
    public void upload_sqlpath_list_values(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":{
                     "bar": {
                       "biz":"buz"
                     }
                  }
                }
                """
        );
        //filePath.toFile().deleteOnExit();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","sqlpath","to",testName,"foo","$.foo"},
                new String[]{"add","sqlpath","to",testName,"bar","{foo}:$.bar"},
                new String[]{"add","sqlpath","to",testName,"biz","{bar}:$.biz"},
                new String[]{"list",testName,"nodes",},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 3"),"expect 3 values\n"+result.getOutput());
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"buz\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"buz\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" buz "),"result should contain .bar:" +result.getOutput());
    }
    @Test
    public void upload_list_values_by_node(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":[
                    {
                      "name": "primero",
                      "bar": {
                        "biz": ["one","first"]
                      }
                    },{
                      "name": "segundo",
                      "bar": {
                        "biz": ["two","second"]
                      }
                    }
                  ]
                }
                """
        );
        //filePath.toFile().deleteOnExit();
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo[]"},//this should act like a dataset
                new String[]{"add","jq","to",testName,"name","{foo}:.name"},
                new String[]{"add","jq","to",testName,"bar","{foo}:.bar"},
                new String[]{"add","jq","to",testName,"biz","{bar}:.biz[] + \"-it\""},//this should also split into a dataset
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName,"by","foo"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult last = results.getLast();
        assertTrue(last.getOutput().contains("Count: 2"),"expect to find 2 results by foo");
    }
    @Test
    public void upload_jq_multi_input(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":[
                   { "mem": "1gb", "cpu": 2},
                   { "mem": "2gb", "cpu": 4}
                  ]
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo[]"},
                new String[]{"add","jq","to",testName,"cpu","{foo}:.cpu"},
                new String[]{"add","jq","to",testName,"mem","{foo}:.mem"},
                new String[]{"add","jq","to",testName,"fingerprint","{mem,cpu}:."},
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}

        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 8"));
    }
    @Test
    public void upload_js_multi_input(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":[
                   { "mem": "1gb", "cpu": 2},
                   { "mem": "2gb", "cpu": 4}
                  ]
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo[]"},
                new String[]{"add","jq","to",testName,"cpu","{foo}:.cpu"},
                new String[]{"add","jq","to",testName,"mem","{foo}:.mem"},
                new String[]{"add","js","to",testName,"fingerprint","({mem,cpu})=>({'fromMem':mem,'fromCpu':cpu})"},
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 8"),"expect to find 8 values\n"+result.getOutput());
        assertFalse(result.getOutput().contains("null")||result.getOutput().contains("NULL"),"list values should not contain null\n"+result.getOutput());
    }

    @Test
    public void recalculate_jq_multi_input(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "foo":[
                   { "mem": "1gb", "cpu": 2},
                   { "mem": "2gb", "cpu": 4}
                  ]
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo[]"},
                new String[]{"add","jq","to",testName,"cpu","{foo}:.cpu"},
                new String[]{"add","jq","to",testName,"mem","{foo}:.mem"},
                new String[]{"add","jq","to",testName,"fingerprint","{mem,cpu}:."},
                new String[]{"list",testName,"nodes"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName},
                new String[]{"recalculate",testName},
                new String[]{"list","value","from",testName}

        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 8"));
    }
    @Test
    public void list_values_as_table(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "string":"example",
                  "version":"1.2.3.4",
                  "double":1.3333333333333,
                  "integer":2,
                  "array":[ "uno", { "other":"value"}],
                  "object": { "key" : { "to" : "value" } }
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"str",".string"},
                new String[]{"add","jq","to",testName,"version",".version"},
                new String[]{"add","jq","to",testName,"double",".double"},
                new String[]{"add","jq","to",testName,"integer",".integer"},
                new String[]{"add","jq","to",testName,"array",".array"},
                new String[]{"add","jq","to",testName,"obj",".object"},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName,"as","table"}

        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 6"),"expect to extract 6 values");
        assertTrue(result.getOutput().contains("│ 1.33"),"double should be truncated\n"+result.getOutput());
        assertFalse(result.getOutput().contains("│ 1.333"),"double should be truncated\n"+result.getOutput());
        assertFalse(result.getOutput().contains("\"example\""),"strings should not be quoted\n"+result.getOutput());

    }
    @Test
    public void list_values_as_table_group_by(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        Path filePath = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                { "foo": [{
                  "string":"example",
                  "version":"1.2.3.4",
                  "double":1.3333333333333,
                  "integer":2,
                  "array":[ "uno", { "other":"value"}],
                  "object": { "key" : { "to" : "value" } }
                  }]
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder",testName},
                new String[]{"add","jq","to",testName,"foo",".foo[]"},
                new String[]{"add","jq","to",testName,"str","{foo}:.string"},
                new String[]{"add","jq","to",testName,"version","{foo}:.version"},
                new String[]{"add","jq","to",testName,"double","{foo}:.double"},
                new String[]{"add","jq","to",testName,"integer","{foo}:.integer"},
                new String[]{"add","jq","to",testName,"array","{foo}:.array"},
                new String[]{"add","jq","to",testName,"obj","{foo}:.object"},
                new String[]{"list","nodes","from",testName},
                new String[]{"upload",folder.toString(),"to",testName},
                new String[]{"list","value","from",testName,"by","foo","as","table"}

        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 1"),"expect one entry in the table");
        assertFalse(result.getOutput().contains("1.333"),"double should be truncated");
        assertFalse(result.getOutput().contains("\"example\""),"strings should not be quoted");

    }


}