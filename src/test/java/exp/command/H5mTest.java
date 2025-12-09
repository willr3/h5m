package exp.command;

import exp.provided.SqliteDatasourceConfiguration;
import io.hyperfoil.tools.yaup.AsciiArt;
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
        String path = SqliteDatasourceConfiguration.getPath();
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
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder","foo","/tmp/foo"},
                new String[]{"list","folders"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("foo"),result.getOutput());
    }

    @Test
    public void list_folder(QuarkusMainLauncher launcher) {
        List<LaunchResult> results = run(launcher,
            new String[]{"add","folder","foo","/tmp/foo"},
            new String[]{"add","folder","bar","/tmp/bar"}
        );
        results.forEach(result->{
           assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        for(List<String> command : List.of(List.of("list","folder"),List.of("list","folders"))){
            result = launcher.launch(command.toArray(new String[0]));
            assertEquals(0,result.exitCode(),result.getOutput());
            assertTrue(result.getOutput().contains("foo"),"expect to find foo folder: "+result.getOutput());
            assertTrue(result.getOutput().contains("bar"),"expect to find bar folder: "+result.getOutput());
        }
    }
    @Test
    public void remove_folder(QuarkusMainLauncher launcher) {
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder","foo","/tmp/foo"},
                new String[]{"remove","folder","foo"},
                new String[]{"list","folders"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertFalse(result.getOutput().contains("foo"),"expect to not find foo folder: "+result.getOutput());
    }

    @Test
    public void add_jq_list_node(QuarkusMainLauncher launcher) {
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder","foo","/tmp/foo"},
                new String[]{"add","jq","to","foo","buz",".buz"},
                new String[]{"add","jq","to","foo","bizzing","{buz}:.biz"},
                new String[]{"list","foo","nodes"},
                new String[]{"list","nodes","from","foo"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("biz"),"expect to find biz: "+result.getOutput());
    }
    @Test
    public void remove_node(QuarkusMainLauncher launcher) {
        List<LaunchResult> results = run(launcher,
                new String[]{"add","folder","foo","/tmp/foo"},
                new String[]{"add","jq","to","foo","biz",".biz"},
                new String[]{"list","foo","nodes"},
                new String[]{"remove","node","biz","from","foo"},
                new String[]{"list","foo","nodes"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertFalse(result.getOutput().contains("biz"),"expect to find biz: "+result.getOutput());
    }

    @Test
    public void scan_list_values(QuarkusMainLauncher launcher) throws IOException {
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
                new String[]{"add","folder","test",folder.toString()},
                new String[]{"add","jq","to","test","foo",".foo"},
                new String[]{"add","jq","to","test","bar","{foo}:.bar"},
                new String[]{"add","jq","to","test","biz","{bar}:.biz"},
                new String[]{"list","test","nodes",},
                new String[]{"scan","test"},
                new String[]{"list","value","from","test"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });

        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 3"));
        assertTrue(result.getOutput().contains(" {\"bar\":{\"biz\":\"buz\"}} "),"result should contain .foo:" +result.getOutput());
        assertTrue(result.getOutput().contains(" {\"biz\":\"buz\"} "),"result should contain .bar:" +result.getOutput());
        assertTrue(result.getOutput().contains(" \"buz\" "),"result should contain .bar:" +result.getOutput());
    }
    @Test
    public void scan_list_values_by_node(QuarkusMainLauncher launcher) throws IOException {
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
                new String[]{"add","folder","test",folder.toString()},
                new String[]{"add","jq","to","test","foo",".foo[]"},//this should act like a dataset
                new String[]{"add","jq","to","test","name","{foo}:.name"},
                new String[]{"add","jq","to","test","bar","{foo}:.bar"},
                new String[]{"add","jq","to","test","biz","{bar}:.biz[] + \"-it\""},//this should also split into a dataset
                new String[]{"list","test","nodes"},
                new String[]{"scan","test"},
                new String[]{"list","value","from","test","by","foo"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
    }
    @Test
    public void scan_jq_multi_input(QuarkusMainLauncher launcher) throws IOException {
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
                new String[]{"add","folder","demo",folder.toString()},
                new String[]{"add","jq","to","demo","foo",".foo[]"},
                new String[]{"add","jq","to","demo","cpu","{foo}:.cpu"},
                new String[]{"add","jq","to","demo","mem","{foo}:.mem"},
                new String[]{"add","jq","to","demo","fingerprint","{mem,cpu}:."},
                new String[]{"list","demo","nodes"},
                new String[]{"scan","demo"},
                new String[]{"list","value","from","demo"}

        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 8"));
    }
    @Test
    public void scan_js_multi_input(QuarkusMainLauncher launcher) throws IOException {
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
                new String[]{"add","folder","demo",folder.toString()},
                new String[]{"add","jq","to","demo","foo",".foo[]"},
                new String[]{"add","jq","to","demo","cpu","{foo}:.cpu"},
                new String[]{"add","jq","to","demo","mem","{foo}:.mem"},
                new String[]{"add","js","to","demo","fingerprint","({mem,cpu})=>({'fromMem':mem,'fromCpu':cpu})"},
                new String[]{"list","demo","nodes"},
                new String[]{"scan","demo"},
                new String[]{"list","value","from","demo"}
        );
        results.forEach(result->{
            assertEquals(0,result.exitCode(),result.getOutput());
        });
        LaunchResult result = results.getLast();
        assertTrue(result.getOutput().contains("Count: 8"));
    }
}