package io.hyperfoil.tools.h5m.cli;

import io.hyperfoil.tools.h5m.provided.DatasourceConfiguration;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

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

    //disabled so it doesn't fail a build
    //This test requires a running Horreum backup on port 6000 with username / password = horreum / horreum
    @Test @Disabled
    public void loadLegacyTests(QuarkusMainLauncher launcher){
        LaunchResult result = null;
        result = launcher.launch("load-legacy-tests","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
        System.out.println("exitCode="+result.exitCode());
        assertEquals(0,result.exitCode());

    }
    @Test @Disabled
    public void loadLegacyRuns(QuarkusMainLauncher launcher){
        LaunchResult result = null;
        result = launcher.launch("load-legacy-tests","testId=391","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
        assertEquals(0,result.exitCode());
        result = launcher.launch("load-legacy-runs","testId=391","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
        System.out.println("exitCode="+result.exitCode());
        assertEquals(0,result.exitCode());
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
                new String[]{"upload",filePath01.toString(),"to",testName},
                new String[]{"upload",filePath02.toString(),"to",testName},
                new String[]{"upload",filePath03.toString(),"to",testName},
                new String[]{"list","value","from",testName}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult last = results.getLast();
        assertTrue(last.getOutput().contains("Count: 13"),"expect 13 values from test");
    }
    @Disabled("There should be only changes detected for x = 2 and x = 12 but there are two other detected for x = 3 and x = 13")
    @Test
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
                new String[]{"upload",filePath01.toString(),"to",testName},
                new String[]{"upload",filePath02.toString(),"to",testName},
                new String[]{"upload",filePath03.toString(),"to",testName},
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
    public void calculate_fixedthreshold_node(QuarkusMainLauncher launcher) throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");
        // value 5.0 is below min=10
        Path filePath01 = Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "y": 5.0, "fp1": "alpha"
                }
                """
        );
        // value 50.0 is within [10, 100]
        Path filePath02 = Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "y": 50.0, "fp1": "alpha"
                }
                """
        );
        // value 150.0 is above max=100
        Path filePath03 = Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "y": 150.0, "fp1": "alpha"
                }
                """
        );
        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "rangeNode", ".y"},
                new String[]{"add", "jq", "to", testName, "fp1", ".fp1"},
                new String[]{"list", testName, "nodes"},
                new String[]{"add", "fixedthreshold", "ftNode", "to", testName, "range", "rangeNode", "fingerprint", "fp1", "min", "10", "max", "100"},
                new String[]{"list", testName, "nodes"},
                new String[]{"upload", folder.toString(), "to", testName},
                new String[]{"list", "value", "from", testName}
        );
        results.forEach(result -> {
            assertEquals(0, result.exitCode(), result.getOutput());
        });

        LaunchResult last = results.getLast();
        // 3 rangeNode values + 3 fp1 values + 3 _fp-ftNode values + 2 fixedthreshold violations = 11
        assertTrue(last.getOutput().contains("Count: 11"), "expect 11 values from test\n" + last.getOutput());
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
    private Path createFixedThresholdSplitData() throws IOException {
        Path folder = Files.createTempDirectory("h5m");
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "items": [
                    {"x": "item1", "y": 20.0, "fp1": "alpha"},
                    {"x": "item2", "y": 175.0, "fp1": "beta"}
                  ]
                }
                """
        );
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "items": [
                    {"x": "item1", "y": 5.0, "fp1": "alpha"},
                    {"x": "item2", "y": 150.0, "fp1": "beta"}
                  ]
                }
                """
        );
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                {
                  "items": [
                    {"x": "item1", "y": 70.0, "fp1": "alpha"},
                    {"x": "item2", "y": 100.0, "fp1": "beta"}
                  ]
                }
                """
        );
        return folder;
    }

    @Test
    public void calculate_fixedthreshold_with_multiple_parent_values(QuarkusMainLauncher launcher) throws IOException {
        String testName = "calculate_fixedthreshold_with_multiple_parent_values";
        Path folder = createFixedThresholdSplitData();

        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "itemSplit", ".items[]"},
                new String[]{"add", "jq", "to", testName, "itemName", "{itemSplit}:.x"},
                new String[]{"add", "jq", "to", testName, "rangeNode", "{itemSplit}:.y"},
                new String[]{"add", "jq", "to", testName, "categoryFp", "{itemSplit}:.fp1"},
                new String[]{"add", "fixedthreshold", "ftNode", "to", testName,
                        "range", "rangeNode", "by", "itemSplit", "fingerprint", "categoryFp", "min", "10", "max", "100"},
                new String[]{"upload", folder.toString(), "to", testName},
                new String[]{"list", "value", "from", testName}
        );

        results.forEach(result -> assertEquals(0, result.exitCode(), result.getOutput()));

        LaunchResult last = results.getLast();
        String output = last.getOutput();

        assertTrue(output.contains("Count: 33"), "Expected 33 total values\n" + output);

        // Scoping via 'by itemSplit': alpha fingerprint matched to item1 range values only
        assertTrue(output.contains("\"y\":20,\"fp1\":\"alpha\""), "Alpha should scope to y=20");
        assertTrue(output.contains("\"y\":5,\"fp1\":\"alpha\""), "Alpha should scope to y=5");
        assertTrue(output.contains("\"y\":70,\"fp1\":\"alpha\""), "Alpha should scope to y=70");

        // Scoping via 'by itemSplit': beta fingerprint matched to item2 range values only
        assertTrue(output.contains("\"y\":175,\"fp1\":\"beta\""), "Beta should scope to y=175");
        assertTrue(output.contains("\"y\":150,\"fp1\":\"beta\""), "Beta should scope to y=150");
        assertTrue(output.contains("\"y\":100,\"fp1\":\"beta\""), "Beta should scope to y=100");
    }

    @Test
    public void calculate_fixedthreshold_with_multiple_parent_values_with_by_split(QuarkusMainLauncher launcher) throws IOException {
        String testName = "calculate_fixedthreshold_with_multiple_parent_values_with_by_split";
        Path folder = createFixedThresholdSplitData();

        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "itemSplit", ".items[]"},
                new String[]{"add", "jq", "to", testName, "itemName", "{itemSplit}:.x"},
                new String[]{"add", "jq", "to", testName, "rangeNode", "{itemSplit}:.y"},
                new String[]{"add", "jq", "to", testName, "categoryFp", "{itemSplit}:.fp1"},
                new String[]{"add", "fixedthreshold", "ftNode", "to", testName,
                        "range", "rangeNode", "by", "itemSplit", "fingerprint", "categoryFp", "min", "10", "max", "100"},
                new String[]{"upload", folder.toString(), "to", testName},
                new String[]{"list", "value", "from", testName, "by", "itemSplit"}
        );

        results.forEach(result -> assertEquals(0, result.exitCode(), result.getOutput()));

        LaunchResult last = results.getLast();
        String output = last.getOutput();

        // With 'by itemSplit', violations are parented under itemSplit values.
        // Listing by itemSplit merges descendants into grouped rows, so violation
        // data (below/above) should appear within the grouped output.
        assertTrue(output.contains("Count: 6"), "Expected 6 groups (2 items x 3 uploads)\n" + output);
        assertTrue(output.contains("below"), "Grouped output should contain below-threshold violation\n" + output);
        assertTrue(output.contains("above"), "Grouped output should contain above-threshold violation\n" + output);
    }

    private String qvssPath(String filename) {
        return new File(Objects.requireNonNull(
                getClass().getClassLoader().getResource("qvss/" + filename)).getFile()
        ).getAbsolutePath();
    }

    @Test
    public void fixedthreshold_qvss_throughput(QuarkusMainLauncher launcher) {
        String testName = "fixedthreshold_qvss_throughput";

        // Throughput values (quarkus3-jvm avThroughput):
        // 27405: 2203 (below), 27406: 2206 (below), 27271: 8778 (below), 27272: 9223 (below)
        // 26594: 29482 (ok), 26598: 29715 (ok), 27279: 29490 (ok), 27897: 29576 (ok)
        // 84315: 88777 (above)
        // Threshold: min=10000, max=35000
        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "throughput", ".results.\"quarkus3-jvm\".load.avThroughput"},
                new String[]{"add", "jq", "to", testName, "version", ".config.QUARKUS_VERSION"},
                new String[]{"add", "fixedthreshold", "ftNode", "to", testName,
                        "range", "throughput",
                        "fingerprint", "version",
                        "min", "10000",
                        "max", "35000"},
                new String[]{"list", testName, "nodes"},
                new String[]{"upload", qvssPath("27405.json"), "to", testName},
                new String[]{"upload", qvssPath("27406.json"), "to", testName},
                new String[]{"upload", qvssPath("27271.json"), "to", testName},
                new String[]{"upload", qvssPath("27272.json"), "to", testName},
                new String[]{"upload", qvssPath("26594.json"), "to", testName},
                new String[]{"upload", qvssPath("26598.json"), "to", testName},
                new String[]{"upload", qvssPath("27279.json"), "to", testName},
                new String[]{"upload", qvssPath("27897.json"), "to", testName},
                new String[]{"upload", qvssPath("84315.json"), "to", testName},
                new String[]{"list", "value", "from", testName}
        );

        results.forEach(result -> {
            assertEquals(0, result.exitCode(), result.getOutput());
        });

        LaunchResult last = results.getLast();
        String output = last.getOutput();

        // 4 below (2203, 2206, 8778, 9223) + 1 above (88777) = 5 violations
        assertTrue(output.contains("below"), "should detect below-threshold violations\n" + output);
        assertTrue(output.contains("above"), "should detect above-threshold violation\n" + output);
    }

    @Test
    public void relativedifference_qvss_throughput_regression(QuarkusMainLauncher launcher) {
        String testName = "relativedifference_qvss_throughput_regression";

        // 9 files from Quarkus 3.7.x, chronological order, shared fingerprint "3.7":
        // 26594: 3.7.1 tp=29482  (2024-02-02) — baseline
        // 26598: 3.7.1 tp=29715  (2024-02-02) — stable
        // 26599: 3.7.1 tp=29561  (2024-02-02) — stable
        // 26776: 3.7.1 tp=29583  (2024-02-07) — stable
        // 27271: 3.7.3 tp=8778   (2024-02-19) — big regression (~70% drop)
        // 27272: 3.7.3 tp=9223   (2024-02-19) — still low
        // 27279: 3.7.3 tp=29490  (2024-02-19) — recovery
        // 27405: 3.7.4 tp=2203   (2024-02-22) — severe regression (~93% drop)
        // 27406: 3.7.4 tp=2206   (2024-02-22) — still low
        // Fingerprint: major.minor version extracted via split/join → "3.7"
        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "throughput", ".results.\"quarkus3-jvm\".load.avThroughput"},
                new String[]{"add", "jq", "to", testName, "majorMinor", ".config.QUARKUS_VERSION | split(\".\") | .[0:2] | join(\".\")"},
                new String[]{"add", "jq", "to", testName, "startTime", ".timing.start"},
                new String[]{"add", "relativedifference", "rdNode", "to", testName,
                        "range", "throughput",
                        "domain", "startTime",
                        "fingerprint", "majorMinor",
                        "window", "1",
                        "minPrevious", "3",
                        "threshold", "0.2"},
                new String[]{"list", testName, "nodes"},
                new String[]{"upload", qvssPath("26594.json"), "to", testName},
                new String[]{"upload", qvssPath("26598.json"), "to", testName},
                new String[]{"upload", qvssPath("26599.json"), "to", testName},
                new String[]{"upload", qvssPath("26776.json"), "to", testName},
                new String[]{"upload", qvssPath("27271.json"), "to", testName},
                new String[]{"upload", qvssPath("27272.json"), "to", testName},
                new String[]{"upload", qvssPath("27279.json"), "to", testName},
                new String[]{"upload", qvssPath("27405.json"), "to", testName},
                new String[]{"upload", qvssPath("27406.json"), "to", testName},
                new String[]{"list", "value", "from", testName}
        );

        results.forEach(result -> {
            assertEquals(0, result.exitCode(), result.getOutput());
        });

        LaunchResult last = results.getLast();
        String output = last.getOutput();

        // Detections expected when enough history builds up:
        // The 29583→8778 drop (~70%) and 29490→2203 drop (~93%) should trigger detection
        assertTrue(output.contains("ratio"), "should detect throughput regression via relative difference\n" + output);
    }

    @Test
    public void fixedthreshold_qvss_split_by_framework(QuarkusMainLauncher launcher) {
        String testName = "fixedthreshold_qvss_split_by_framework";

        // 6 files with both quarkus-jvm and spring-jvm results:
        // 7691:  quarkus=28795, spring=10714
        // 7750:  quarkus=28904, spring=10758
        // 6313:  quarkus=32798, spring=11088
        // 6314:  quarkus=37391, spring=12269
        // 16328: quarkus=28829, spring=9431
        // 17333: quarkus=28772, spring=9700
        // Split on .results | to_entries[], fingerprint on framework key
        // Threshold min=15000: all spring-jvm values violate, no quarkus-jvm values violate
        List<LaunchResult> results = run(launcher,
                new String[]{"add", "folder", testName},
                new String[]{"add", "jq", "to", testName, "framework", ".results | to_entries[]"},
                new String[]{"add", "jq", "to", testName, "throughput", "{framework}:.value.load.avThroughput"},
                new String[]{"add", "jq", "to", testName, "fwName", "{framework}:.key"},
                new String[]{"add", "fixedthreshold", "ftNode", "to", testName,
                        "range", "throughput",
                        "by", "framework",
                        "fingerprint", "fwName",
                        "min", "15000"},
                new String[]{"list", testName, "nodes"},
                new String[]{"upload", qvssPath("7691.json"), "to", testName},
                new String[]{"upload", qvssPath("7750.json"), "to", testName},
                new String[]{"upload", qvssPath("6313.json"), "to", testName},
                new String[]{"upload", qvssPath("6314.json"), "to", testName},
                new String[]{"upload", qvssPath("16328.json"), "to", testName},
                new String[]{"upload", qvssPath("17333.json"), "to", testName},
                new String[]{"list", "value", "from", testName}
        );

        results.forEach(result -> {
            assertEquals(0, result.exitCode(), result.getOutput());
        });

        LaunchResult last = results.getLast();
        String output = last.getOutput();

        // All spring-jvm values (~9400-12200) are below min=15000
        assertTrue(output.contains("below"), "should detect spring below threshold\n" + output);
        assertTrue(output.contains("\"fwName\":\"spring-jvm\""),
                "should have spring-jvm in violation fingerprint\n" + output);

        // All quarkus-jvm values (~28700-37400) are above min=15000
        // No violation should contain quarkus-jvm fingerprint — scoping must keep them separate
        assertFalse(output.contains("\"fingerprint\":{\"fwName\":\"quarkus-jvm\"}"),
                "quarkus-jvm should not appear in violations — scoping broken\n" + output);
    }

}
