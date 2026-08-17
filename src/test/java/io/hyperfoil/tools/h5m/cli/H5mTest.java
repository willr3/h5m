package io.hyperfoil.tools.h5m.cli;


import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import io.quarkus.test.aesh.AeshLauncher;
import io.quarkus.test.aesh.AeshLauncherImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusMainTest
@TestProfile(CliProfile.class)
public class H5mTest {

    private AeshLauncher aeshLauncher;

    private static final Duration CMD_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Convert a String[] of args into a single command string for the REPL.
     * Arguments containing spaces, newlines, or special characters are quoted.
     * Uses single quotes for arguments with double quotes (aesh doesn't handle
     * backslash-escaped quotes inside double-quoted strings).
     */
    private static String toCommand(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) sb.append(' ');
            String arg = args[i];
            boolean needsQuoting = arg.contains(" ") || arg.contains("\n") || arg.contains("\"") || arg.contains("{") || arg.contains("}") || arg.contains("|");
            if (!needsQuoting) {
                sb.append(arg);
            } else if (arg.contains("\"") && !arg.contains("'")) {
                // Use single quotes to preserve double quotes literally
                sb.append("'").append(arg).append("'");
            } else {
                // Default: double-quote and escape internal double quotes
                sb.append('"').append(arg.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")).append('"');
            }
        }
        return sb.toString();
    }

    public static List<String> run(AeshLauncher launcher, String[]... args) {
        List<String> outputs = new ArrayList<>();
        for (String[] arg : args) {
            // Brief pause before each command to allow aesh's readline cycle
            // to set up stdinHandler after processing the previous command.
            // Without this, command bytes may arrive while stdinHandler is null
            // (between readline cycles) and be silently dropped.
            // See: https://github.com/aeshell/aesh-readline/issues/233
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            String command = toCommand(arg);
            long start = System.currentTimeMillis();
            String output = launcher.executeCommand(command, CMD_TIMEOUT);
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("run (%dms): %s%n", elapsed, command);
            outputs.add(output);
        }
        // Additional pause after the last command before Quarkus shuts down
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return outputs;
    }

    @BeforeEach
    public void setup(QuarkusMainLauncher launcher) {
        ///tmp/h5m-test.db-shm, /tmp/h5m-test.db-wal, /tmp/h5m-test.db
        String path = CliProfile.TEST_DB_PATH;
        List.of("","-shm","-wal").forEach(suffix->{
            File f = new File(path+suffix);
            if(f.exists()){
                f.delete();
            }
        });
        aeshLauncher = new AeshLauncherImpl(launcher);
        aeshLauncher.launch();
        // Brief pause after launch to allow aesh's readline to set up
        // stdinHandler before the first command is sent.
        // See: https://github.com/aeshell/aesh-readline/issues/233
        try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @AfterEach
    public void teardown() {
        if (aeshLauncher != null) {
            aeshLauncher.exit();
        }
    }

    //disabled so it doesn't fail a build
    //This test requires a running Horreum backup on port 6000 with username / password = horreum / horreum
    @Test @Disabled
    public void loadLegacyTests(){
        String output = aeshLauncher.executeCommand("legacy load-tests username=horreum password=horreum url=jdbc:postgresql://0.0.0.0:6000/horreum", CMD_TIMEOUT);
        System.out.println("output="+output);
        assertNotNull(output);
    }
    @Test @Disabled
    public void loadLegacyRuns(){
        aeshLauncher.executeCommand("legacy load-tests testId=391 username=horreum password=horreum url=jdbc:postgresql://0.0.0.0:6000/horreum", CMD_TIMEOUT);
        String output = aeshLauncher.executeCommand("legacy load-runs testId=391 username=horreum password=horreum url=jdbc:postgresql://0.0.0.0:6000/horreum", CMD_TIMEOUT);
        System.out.println("output="+output);
        assertNotNull(output);
    }
    @Test @Disabled
    public void veritaserum(QuarkusMainLauncher launcher){
        LaunchResult result = null;
//        result = launcher.launch("load-legacy-tests","testId=339","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
//        assertEquals(0,result.exitCode());
//        result = launcher.launch("load-legacy-runs","testId=339","limit=1","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
//        assertEquals(0,result.exitCode());

//        result = launcher.launch("veritaserum","--testId=339","limit=2","--runId=214280","username=horreum","password=horreum","url=jdbc:postgresql://0.0.0.0:6000/horreum");
//        System.out.println("exitCode="+result.exitCode());
//        assertEquals(0,result.exitCode());

        String output = aeshLauncher.executeCommand("veritaserum --testId=339 limit=2 --runId=214280 username=horreum password=horreum url=jdbc:postgresql://0.0.0.0:6000/horreum", CMD_TIMEOUT);

    }

    @Test
    public void list() {
        String output = aeshLauncher.executeCommand("folder list", CMD_TIMEOUT);
        assertNotNull(output);
    }

    @Test @Disabled("help built-in command output not captured by AeshLauncherImpl")
    public void help() {
        String output = aeshLauncher.executeCommand("help", CMD_TIMEOUT);
        assertNotNull(output);
    }

    @Test
    public void add_folder() {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"folder","list"}
        );
        String output = results.getLast();
        assertTrue(output.contains(testName),output);
    }

    @Test
    public void list_folder() {
        List<String> results = run(aeshLauncher,
            new String[]{"folder","add","foo"},
            new String[]{"folder","add","bar"}
        );
        for(List<String> command : List.of(List.of("folder","list"),List.of("folder","list"))){
            String output = aeshLauncher.executeCommand(String.join(" ", command), CMD_TIMEOUT);
            assertTrue(output.contains("foo"),"expect to find foo folder:\n"+output);
            assertTrue(output.contains("bar"),"expect to find bar folder:\n"+output);
        }
    }
    @Test
    public void remove_folder() {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"folder","remove",testName},
                new String[]{"folder","list"}
        );
        String output = results.getLast();
        assertFalse(output.contains(testName),"expect to not find foo folder: "+output);
    }
    @Test
    public void add_js_uses_other_nodes() {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".buz"},
                new String[]{"node","add","jq","bar",".bar"},
                new String[]{"node","add","jq","biz",".biz"},
                new String[]{"node","add","js","dataset","function* dataset({foo, bar, biz}){\nyield foo;\nyield bar;\nyield biz;\n}"},
                new String[]{"node","list"},
                new String[]{"node","list"},
                new String[]{"cd",".."}
        );
        // All commands should succeed without throwing
    }
    @Test
    public void add_jq_list_node() {
        // Uses explicit --to/--from (no cd) to verify that path works
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"node","add","jq","--to",testName,"buz",".buz"},
                new String[]{"node","add","jq","--to",testName,"bizzing","{buz}:.biz"},
                new String[]{"node","list","--from",testName},
                new String[]{"node","list","--from",testName}
        );
        String output = results.getLast();
        assertTrue(output.contains("biz"),"expect to find biz: "+output);
    }
    @Test
    public void add_relativedifference_list_node() {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","domainNode",".x"},
                new String[]{"node","add","jq","rangeNode",".y"},
                new String[]{"node","add","jq","fp1",".fp1"},
                new String[]{"node","add","jq","fp2",".fp2"},
                new String[]{"node","list"},
                new String[]{"node","add","relativedifference","rd1","--range","rangeNode","--domain","domainNode","--fingerprint","fp1,fp2"},
                new String[]{"node","list"},
                new String[]{"cd",".."}
        );
        String output = results.get(results.size() - 2);
        assertTrue(output.contains("rd1"),"expect to find rd1: "+output);



    }
    @Test
    public void calculate_relativedifference_node() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","domainNode",".x"},
                new String[]{"node","add","jq","rangeNode",".y"},
                new String[]{"node","add","jq","fp1",".fp1"},
                new String[]{"node","list"},
                new String[]{"node","add","relativedifference","relativediff","--range","rangeNode","--domain","domainNode","--fingerprint","fp1","--window","1","--minPrevious","1"},
                new String[]{"node","list"},
                new String[]{"run", "upload",filePath01.toString()},
                new String[]{"run", "upload",filePath02.toString()},
                new String[]{"run", "upload",filePath03.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String last = results.get(results.size() - 2);
        assertTrue(last.contains("Count: 13"),"expect 13 values from test");
    }
    @Disabled("There should be only changes detected for x = 2 and x = 12 but there are two other detected for x = 3 and x = 13")
    @Test
    public void calculate_relativedifference_dataset_node() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","split",".each[]"},
                new String[]{"node","add","jq","domainNode","{split}:.x"},
                new String[]{"node","add","jq","rangeNode","{split}:.y"},
                new String[]{"node","add","jq","fp1","{split}:.fp1"},
                new String[]{"node","add","jq","fp2","{split}:.fp2"},
                new String[]{"node","list"},
                new String[]{"node","add","relativedifference","relativediff","--range","rangeNode","--domain","domainNode","--by","split","--fingerprint","fp1,fp2","--window","1","--minPrevious","1"},
                new String[]{"node","list"},
                new String[]{"run", "upload",filePath01.toString()},
                new String[]{"run", "upload",filePath02.toString()},
                new String[]{"run", "upload",filePath03.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String last = results.get(results.size() - 2);
        assertTrue(last.contains("Count: 38"),"expect 38 values from test");
    }

    @Test
    public void remove_node() {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","biz",".biz"},
                new String[]{"node","list"},
                new String[]{"node","remove","biz"},
                new String[]{"node","list"},
                new String[]{"cd",".."}
        );
        String output = results.get(results.size() - 2);
        assertFalse(output.contains("biz"),"expect to NOT find biz: "+output);
    }

    @Test
    public void upload_list_values() throws IOException {
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
        // Uses explicit --to/--from (no cd) to verify that path works
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"node","add","jq","--to",testName,"foo",".foo"},
                new String[]{"node","add","jq","--to",testName,"bar","{foo}:.bar"},
                new String[]{"node","add","jq","--to",testName,"biz","{bar}:.biz"},
                new String[]{"node","list","--from",testName},
                new String[]{"run", "upload",folder.toString(),"--to",testName},
                new String[]{"folder","values","--from",testName},
                new String[]{"cd",testName}
        );

        String output = results.get( results.size()-2);
        assertTrue(output.contains("Count: 3"), output);
        // Intermediate nodes (foo, bar) are ephemeral — their data is nullified after processing.
        // Only the leaf node (biz) retains its data.
        assertTrue(output.contains(" buz "),"result should contain leaf value buz:" +output);
    }
    @Test
    public void upload_folder_list_values() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo"},
                new String[]{"node","add","jq","bar","{foo}:.bar"},
                new String[]{"node","add","jq","biz","{bar}:.biz"},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 6"));
        // Intermediate nodes (foo, bar) are ephemeral — their data is nullified after processing.
        // Only the leaf node (biz) retains its data.
        assertTrue(output.contains(" buz "),"result should contain leaf value buz:" +output);
        assertTrue(output.contains(" bur "),"result should contain leaf value bur:" +output);
    }
    @Test
    public void upload_jsonata_list_values() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jsonata","foo","foo"},
                new String[]{"node","add","jsonata","bar","{foo}:bar"},
                new String[]{"node","add","jsonata","biz","{bar}:biz"},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 3"),"expect 3 values\n"+output);
        // Intermediate nodes (foo, bar) are ephemeral — only leaf node (biz) retains data.
        assertTrue(output.contains(" buz "),"result should contain leaf value buz:" +output);
    }
    @Test
    public void upload_sqlpath_list_values() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo"},
                new String[]{"node","add","jq","bar","{foo}:.bar"},
                new String[]{"node","add","jq","biz","{bar}:.biz"},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 3"),"expect 3 values\n"+output);
        // Intermediate nodes (foo, bar) are ephemeral — only leaf node (biz) retains data.
        assertTrue(output.contains(" buz "),"result should contain leaf value buz:" +output);
    }
    @Test
    public void upload_list_values_by_node() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo[]"},//this should act like a dataset
                new String[]{"node","add","jq","name","{foo}:.name"},
                new String[]{"node","add","jq","bar","{foo}:.bar"},
                new String[]{"node","add","jq","biz","{bar}:.biz[] + \"-it\""},//this should also split into a dataset
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values","--by","foo"},
                new String[]{"cd",".."}
        );

        String last = results.get(results.size() - 2);
        assertTrue(last.contains("Count: 2"),"expect to find 2 results by foo");
    }
    @Test
    public void upload_jq_multi_input() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo[]"},
                new String[]{"node","add","jq","cpu","{foo}:.cpu"},
                new String[]{"node","add","jq","mem","{foo}:.mem"},
                new String[]{"node","add","fingerprint","fp","{mem,cpu}:."},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}

        );
        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 8"));
    }
    @Test
    public void upload_js_multi_input() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo[]"},
                new String[]{"node","add","jq","cpu","{foo}:.cpu"},
                new String[]{"node","add","jq","mem","{foo}:.mem"},
                new String[]{"node","add","fingerprint","fp","{mem,cpu}:."},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );
        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 8"),"expect to find 8 values\n"+output);
    }

    @Test
    public void recalculate_jq_multi_input() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo[]"},
                new String[]{"node","add","jq","cpu","{foo}:.cpu"},
                new String[]{"node","add","jq","mem","{foo}:.mem"},
                new String[]{"node","add","fingerprint","fp","{mem,cpu}:."},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values"},
                new String[]{"folder","recalculate"},
                new String[]{"folder","values"},
                new String[]{"cd",".."}

        );
        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 8"));
    }
    @Test
    public void calculate_fixedthreshold_node() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "rangeNode", ".y"},
                new String[]{"node", "add", "jq", "fp1", ".fp1"},
                new String[]{"node", "list"},
                new String[]{"node", "add", "fixedthreshold", "ftNode", "--range", "rangeNode", "--fingerprint", "fp1", "--min", "10", "--max", "100"},
                new String[]{"node", "list"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values"},
                new String[]{"cd", ".."}
        );

        // Upload output should contain the processing ID(s) and detection summary
        String uploadOutput = results.get(7); // upload command is at index 7
        assertTrue(uploadOutput.contains("processing id:"), "upload should show processing id\n" + uploadOutput);
        assertTrue(uploadOutput.contains("Processing complete."), "upload should show completion\n" + uploadOutput);
        assertTrue(uploadOutput.contains("2 changes detected"), "upload should show 2 changes\n" + uploadOutput);
        assertTrue(uploadOutput.contains("FIXED_THRESHOLD"), "upload should show detection type\n" + uploadOutput);
        assertTrue(uploadOutput.contains("fingerprint="), "upload should show fingerprint\n" + uploadOutput);

        String last = results.get(results.size() - 2);
        // 3 rangeNode values + 3 fp1 values + 3 _fp-ftNode values + 2 fixedthreshold violations = 11
        assertTrue(last.contains("Count: 11"), "expect 11 values from test\n" + last);
    }

    @Test
    public void list_values_as_table() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","str",".string"},
                new String[]{"node","add","jq","version",".version"},
                new String[]{"node","add","jq","double",".double"},
                new String[]{"node","add","jq","integer",".integer"},
                new String[]{"node","add","jq","array",".array"},
                new String[]{"node","add","jq","obj",".object"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values","--as","table"},
                new String[]{"cd",".."}

        );

        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 6"),"expect to extract 6 values");
        assertTrue(output.contains("│ 1.33"),"double should be truncated\n"+output);
        assertFalse(output.contains("│ 1.333"),"double should be truncated\n"+output);
        assertFalse(output.contains("\"example\""),"strings should not be quoted\n"+output);

    }
    @Test
    public void list_values_as_table_group_by() throws IOException {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","foo",".foo[]"},
                new String[]{"node","add","jq","str","{foo}:.string"},
                new String[]{"node","add","jq","version","{foo}:.version"},
                new String[]{"node","add","jq","double","{foo}:.double"},
                new String[]{"node","add","jq","integer","{foo}:.integer"},
                new String[]{"node","add","jq","array","{foo}:.array"},
                new String[]{"node","add","jq","obj","{foo}:.object"},
                new String[]{"node","list"},
                new String[]{"run", "upload",folder.toString()},
                new String[]{"folder","values","--by","foo","--as","table"},
                new String[]{"cd",".."}

        );

        String output = results.get(results.size() - 2);
        assertTrue(output.contains("Count: 1"),"expect one entry in the table");
        assertFalse(output.contains("1.333"),"double should be truncated");
        assertFalse(output.contains("\"example\""),"strings should not be quoted");

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
    public void calculate_fixedthreshold_with_multiple_parent_values() throws IOException {
        String testName = "calculate_fixedthreshold_with_multiple_parent_values";
        Path folder = createFixedThresholdSplitData();

        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "itemSplit", ".items[]"},
                new String[]{"node", "add", "jq", "itemName", "{itemSplit}:.x"},
                new String[]{"node", "add", "jq", "rangeNode", "{itemSplit}:.y"},
                new String[]{"node", "add", "jq", "categoryFp", "{itemSplit}:.fp1"},
                new String[]{"node", "add", "fixedthreshold", "ftNode",
                        "--range", "rangeNode", "--by", "itemSplit", "--fingerprint", "categoryFp", "--min", "10", "--max", "100"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values"},
                new String[]{"cd", ".."}
        );

        String last = results.get(results.size() - 2);
        String output = last;

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
    public void calculate_fixedthreshold_with_multiple_parent_values_with_by_split() throws IOException {
        String testName = "calculate_fixedthreshold_with_multiple_parent_values_with_by_split";
        Path folder = createFixedThresholdSplitData();

        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "itemSplit", ".items[]"},
                new String[]{"node", "add", "jq", "itemName", "{itemSplit}:.x"},
                new String[]{"node", "add", "jq", "rangeNode", "{itemSplit}:.y"},
                new String[]{"node", "add", "jq", "categoryFp", "{itemSplit}:.fp1"},
                new String[]{"node", "add", "fixedthreshold", "ftNode",
                        "--range", "rangeNode", "--by", "itemSplit", "--fingerprint", "categoryFp", "--min", "10", "--max", "100"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values", "--by", "itemSplit"},
                new String[]{"cd", ".."}
        );

        String last = results.get(results.size() - 2);
        String output = last;

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
    public void fixedthreshold_qvss_throughput() {
        String testName = "fixedthreshold_qvss_throughput";

        // Throughput values (quarkus3-jvm avThroughput):
        // 27405: 2203 (below), 27406: 2206 (below), 27271: 8778 (below), 27272: 9223 (below)
        // 26594: 29482 (ok), 26598: 29715 (ok), 27279: 29490 (ok), 27897: 29576 (ok)
        // 84315: 88777 (above)
        // Threshold: min=10000, max=35000
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "throughput", ".results.\"quarkus3-jvm\".load.avThroughput"},
                new String[]{"node", "add", "jq", "version", ".config.QUARKUS_VERSION"},
                new String[]{"node", "add", "fixedthreshold", "ftNode",
                        "--range", "throughput",
                        "--fingerprint", "version",
                        "--min", "10000",
                        "--max", "35000"},
                new String[]{"node", "list"},
                new String[]{"run", "upload", qvssPath("27405.json")},
                new String[]{"run", "upload", qvssPath("27406.json")},
                new String[]{"run", "upload", qvssPath("27271.json")},
                new String[]{"run", "upload", qvssPath("27272.json")},
                new String[]{"run", "upload", qvssPath("26594.json")},
                new String[]{"run", "upload", qvssPath("26598.json")},
                new String[]{"run", "upload", qvssPath("27279.json")},
                new String[]{"run", "upload", qvssPath("27897.json")},
                new String[]{"run", "upload", qvssPath("84315.json")},
                new String[]{"folder", "values"},
                new String[]{"cd", ".."}
        );

        String last = results.get(results.size() - 2);
        String output = last;

        // 4 below (2203, 2206, 8778, 9223) + 1 above (88777) = 5 violations
        assertTrue(output.contains("below"), "should detect below-threshold violations\n" + output);
        assertTrue(output.contains("above"), "should detect above-threshold violation\n" + output);
    }

    @Test
    public void relativedifference_qvss_throughput_regression() {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "throughput", ".results.\"quarkus3-jvm\".load.avThroughput"},
                new String[]{"node", "add", "jq", "majorMinor", ".config.QUARKUS_VERSION | split(\".\") | .[0:2] | join(\".\")"},
                new String[]{"node", "add", "jq", "startTime", ".timing.start"},
                new String[]{"node", "add", "relativedifference", "rdNode",
                        "--range", "throughput",
                        "--domain", "startTime",
                        "--fingerprint", "majorMinor",
                        "--window", "1",
                        "--minPrevious", "3",
                        "--threshold", "0.2"},
                new String[]{"node", "list"},
                new String[]{"run", "upload", qvssPath("26594.json")},
                new String[]{"run", "upload", qvssPath("26598.json")},
                new String[]{"run", "upload", qvssPath("26599.json")},
                new String[]{"run", "upload", qvssPath("26776.json")},
                new String[]{"run", "upload", qvssPath("27271.json")},
                new String[]{"run", "upload", qvssPath("27272.json")},
                new String[]{"run", "upload", qvssPath("27279.json")},
                new String[]{"run", "upload", qvssPath("27405.json")},
                new String[]{"run", "upload", qvssPath("27406.json")},
                new String[]{"folder", "values"},
                new String[]{"cd", ".."}
        );

        String last = results.get(results.size() - 2);
        String output = last;

        // Detections expected when enough history builds up:
        // The 29583→8778 drop (~70%) and 29490→2203 drop (~93%) should trigger detection
        assertTrue(output.contains("ratio"), "should detect throughput regression via relative difference\n" + output);
    }

    @Test
    public void fixedthreshold_qvss_split_by_framework() {
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
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "framework", ".results | to_entries[]"},
                new String[]{"node", "add", "jq", "throughput", "{framework}:.value.load.avThroughput"},
                new String[]{"node", "add", "jq", "fwName", "{framework}:.key"},
                new String[]{"node", "add", "fixedthreshold", "ftNode",
                        "--range", "throughput",
                        "--by", "framework",
                        "--fingerprint", "fwName",
                        "--min", "15000"},
                new String[]{"node", "list"},
                new String[]{"run", "upload", qvssPath("7691.json")},
                new String[]{"run", "upload", qvssPath("7750.json")},
                new String[]{"run", "upload", qvssPath("6313.json")},
                new String[]{"run", "upload", qvssPath("6314.json")},
                new String[]{"run", "upload", qvssPath("16328.json")},
                new String[]{"run", "upload", qvssPath("17333.json")},
                new String[]{"folder", "values", "--limit", "200"},
                new String[]{"cd", ".."}
        );

        String last = results.get(results.size() - 2);
        String output = last;

        // All spring-jvm values (~9400-12200) are below min=15000
        assertTrue(output.contains("below"), "should detect spring below threshold\n" + output);
        assertTrue(output.contains("\"fwName\":\"spring-jvm\""),
                "should have spring-jvm in violation fingerprint\n" + output);

        // All quarkus-jvm values (~28700-37400) are above min=15000
        // No violation should contain quarkus-jvm fingerprint — scoping must keep them separate
        assertFalse(output.contains("\"fingerprint\":{\"fwName\":\"quarkus-jvm\"}"),
                "quarkus-jvm should not appear in violations — scoping broken\n" + output);
    }

    @Test
    public void relativedifference_not_recalculate_old_changes() throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");

        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 3, "y": 1.1, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 2, "y": 2.1, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath03 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 1, "y": 3.1, "fp": "alpha"}
                  ]
                }
                """
        );

        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","split",".Item[]"},
                new String[]{"node","add","jq","domainNode","{split}:.x"},
                new String[]{"node","add","jq","rangeNode","{split}:.y"},
                new String[]{"node","add","jq","fp","{split}:.fp"},
                new String[]{"node","list"},
                new String[]{"node","add","relativedifference","relativediff","--range","rangeNode","--domain","domainNode","--by","split","--fingerprint","fp","--window","1","--minPrevious","1"},
                new String[]{"run", "upload",filePath01.toString()},
                new String[]{"run", "upload",filePath02.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath03.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String afterUpload2 = results.get(results.size() - 4);
        String output2 = afterUpload2;
        assertTrue(afterUpload2.contains("Count: 11"),
                "After upload 2, expect 11 values from test (1 changes total)\n" + afterUpload2);
        assertTrue(output2.contains("\"domainvalue\":3"),
                "Change should be detected for domain x=4\n" + output2);

        assertTrue(output2.contains("\"previous\":2.1"),
                "Should show previous y value of 2.1\n" + output2);
        assertTrue(output2.contains("\"last\":2.1"),
                "Should show last y value of 2.1\n" + output2);
        assertTrue(output2.contains("\"ratio\":-47.61904761904761"),
                "Should contain calculated ratio -47.61904761904761\n" + output2);

        String output3 = results.get(results.size() - 2);
        assertTrue(output3.contains("Count: 17"),
            "After upload 3, expect 17 values from test (2 changes total)\n" + output3);
        assertTrue(output3.contains("\"domainvalue\":2"),
                "Change should be detected for domain x=2\n" + output3);

        assertTrue(output3.contains("\"previous\":3.1"),
                "Should show previous y value of 3.1\n" + output3);
        assertTrue(output3.contains("\"last\":3.1"),
                "Should show last y value of 3.1\n" + output3);
        assertTrue(output3.contains("\"ratio\":-32.258064516129025"),
                "Should contain calculated ratio -32.258064516129025\n" + output3);

    }

    @Test
    public void relativedifference_skip_minPrevious() throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");

        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 4, "y": 1.0, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 3, "y": 1.5, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath03 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 2, "y": 2.0, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath04 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 1, "y": 2.5, "fp": "alpha"}
                  ]
                }
                """
        );

        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","split",".Item[]"},
                new String[]{"node","add","jq","domainNode","{split}:.x"},
                new String[]{"node","add","jq","rangeNode","{split}:.y"},
                new String[]{"node","add","jq","fp","{split}:.fp"},
                new String[]{"node","add","relativedifference","relativediff","--range","rangeNode","--domain","domainNode","--by","split","--fingerprint","fp","--window","1","--minPrevious","2"},
                new String[]{"run", "upload",filePath01.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath02.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath03.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath04.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String output1 = results.get(results.size() - 8);
        assertTrue(output1.contains("Count: 5"),
            "After upload 1, expect 5 values\n" + output1);
        int changeCount1 = output1.split("\"ratio\":", -1).length - 1;
        assertEquals(0, changeCount1,
                "Upload 1: x=4 with only 1 sample should produce 0 changes (need minPrevious=2)");


        String output2 = results.get(results.size() - 6);
        assertTrue(output2.contains("Count: 10"),
            "After upload 2, expect 10 values\n" + output2);

        int changeCount2 = output2.split("\"ratio\":", -1).length - 1;
        assertEquals(0, changeCount2,
                "Upload 1: x=3 with only 1 sample should produce 0 changes (need minPrevious=2)");

        String output3 = results.get(results.size() - 4);
        int changeCount3 = output3.split("\"ratio\":", -1).length - 1;
        assertEquals(1, changeCount3,
                "Upload 1: x=2 with only 1 sample should produce 0 changes (need minPrevious=2)");
        assertTrue(output3.contains("Count: 16"),
            "After upload 3, expect 16 values (expect 1 change here since it violates threshold value)\n" + output3);
        assertTrue(output3.contains("\"domainvalue\":4"),
                "Change should be detected for domain x=4\n" + output3);

        assertTrue(output3.contains("\"previous\":2"),
                "Should show previous y value of 2\n" + output3);
        assertTrue(output3.contains("\"last\":2"),
                "Should show last y value of 2\n" + output3);
        assertTrue(output3.contains("\"ratio\":-42.85714285714286"),
                "Should contain calculated ratio -42.85714285714286\n" + output3);


        String output4 = results.get(results.size() - 2);
        int changeCount4 = output4.split("\"ratio\":", -1).length - 1;
        assertEquals(2, changeCount4,
                "Upload 1: x=1 should have 2 changes\n" + output4);
        assertTrue(output4.contains("Count: 22"),
            "After upload 4, expect 22 values (expect 1 change here since it violates threshold value. With minPrevious=2 total changes=2)\n" + output4);

        assertTrue(output4.contains("\"domainvalue\":3"),
                "Change should be detected for domain x=3\n" + output4);

        assertTrue(output4.contains("\"previous\":2.5"),
                "Should show previous y value of 2.5\n" + output4);
        assertTrue(output4.contains("\"last\":2.5"),
                "Should show last y value of 2.5\n" + output4);
        assertTrue(output4.contains("\"ratio\":-33.333333333333336"),
                "Should contain calculated ratio -33.333333333333336\n" + output4);
    }

    @Test
    public void relativedifference_Unordered_uploads() throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");

        Path filePath01 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 4, "y": 1.1, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath02 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 2, "y": 2.1, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath03 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 3, "y": 3.1, "fp": "alpha"}
                  ]
                }
                """
        );

        Path filePath04 = Files.writeString(Files.createTempFile(folder,"h5m",".json").toAbsolutePath(),
                """
                {
                  "Item": [
                    {"x": 1, "y": 4.1, "fp": "alpha"}
                  ]
                }
                """
        );

        List<String> results = run(aeshLauncher,
                new String[]{"folder","add",testName},
                new String[]{"cd",testName},
                new String[]{"node","add","jq","split",".Item[]"},
                new String[]{"node","add","jq","domainNode","{split}:.x"},
                new String[]{"node","add","jq","rangeNode","{split}:.y"},
                new String[]{"node","add","jq","fp","{split}:.fp"},
                new String[]{"node","list"},
                new String[]{"node","add","relativedifference","relativediff","--range","rangeNode","--domain","domainNode","--by","split","--fingerprint","fp","--window","1","--minPrevious","1"},
                new String[]{"run", "upload",filePath01.toString()},
                new String[]{"run", "upload",filePath02.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath03.toString()},
                new String[]{"folder","values"},
                new String[]{"run", "upload",filePath04.toString()},
                new String[]{"folder","values"},
                new String[]{"cd",".."}
        );

        String output1 = results.get(results.size() - 8);
        int changeCount1 = output1.split("\"ratio\":", -1).length - 1;
        assertEquals(0, changeCount1,
                "Upload 1: x=4 with only 1 sample should produce 0 changes (need minPrevious=2)");


        String output2 = results.get(results.size() - 6);
        assertTrue(output2.contains("Count: 11"),
                "After upload 2, expect 11 values (1 change detected for x=4)\n" + output2);

        assertTrue(output2.contains("\"domainvalue\":4"),
                "Change should be detected for domain x=4\n" + output2);

        assertTrue(output2.contains("\"previous\":2.1"),
                "Should show previous y value of 2.1\n" + output2);
        assertTrue(output2.contains("\"last\":2.1"),
                "Should show last y value of 2.1\n" + output2);
        assertTrue(output2.contains("\"ratio\":-47.61904761904761"),
                "Should contain calculated ratio -47.61904761904761\n" + output2);

        String output3 = results.get(results.size() - 4);
        assertTrue(output3.contains("Count: 16"),
                "After upload 3, expect 16 values (1 changes total)\n" + output3);

        assertTrue(output3.contains("\"domainvalue\":3"),
                "Change should be detected for domain x=3\n" + output3);
        assertTrue(output3.contains("\"previous\":2.1"),
                "Should show previous y value of 2.1\n" + output3);
        assertTrue(output3.contains("\"last\":2.1"),
                "Should show last y value of 2.1\n" + output3);
        assertTrue(output3.contains("\"ratio\":47.61904761904763"),
                "Should contain calculated ratio 47.61904761904763\n" + output3);

        String output4 = results.get(results.size() - 2);
        assertTrue(output4.contains("Count: 22"),
                "After upload 4, expect 22 values (2 changes total)\n" + output4);

        assertTrue(output4.contains("\"domainvalue\":2"),
                "Change should be detected for domain x=2\n" + output4);

        assertTrue(output4.contains("\"previous\":4.1"),
                "Should show previous y value of 4.1\n" + output4);
        assertTrue(output4.contains("\"last\":4.1"),
                "Should show last y value of 4.1\n" + output4);
        assertTrue(output4.contains("\"ratio\":-48.78048780487805"),
                "Should contain calculated ratio -48.78048780487805\n" + output4);
    }

    @Test
    public void calculate_stddev_anomaly_node() throws IOException {
        String testName = StackWalker.getInstance()
                .walk(s -> s.skip(0).findFirst())
                .get()
                .getMethodName();
        Path folder = Files.createTempDirectory("h5m");

        // 5 stable uploads at y=100, then 1 anomaly at y=200
        // Upload files individually in order to guarantee deterministic processing.
        // File.listFiles() does not guarantee order, so directory upload is unreliable.
        List<Path> uploadFiles = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            uploadFiles.add(Files.writeString(folder.resolve(String.format("upload_%02d.json", i)),
                    String.format("""
                    {
                        "x": %d, "y": 100.0, "fp1": "alpha"
                    }
                    """, i)));
        }
        uploadFiles.add(Files.writeString(folder.resolve("upload_06.json"),
                """
                {
                    "x": 6, "y": 200.0, "fp1": "alpha"
                }
                """));

        // Build commands: setup nodes, then upload each file individually in order
        List<String[]> commands = new java.util.ArrayList<>();
        commands.add(new String[]{"folder", "add", testName});
        commands.add(new String[]{"cd", testName});
        commands.add(new String[]{"node", "add", "jq", "domainNode", ".x"});
        commands.add(new String[]{"node", "add", "jq", "rangeNode", ".y"});
        commands.add(new String[]{"node", "add", "jq", "fp1", ".fp1"});
        commands.add(new String[]{"node", "list"});
        commands.add(new String[]{"node", "add", "stddev", "sdNode",
                "range", "rangeNode", "domain", "domainNode",
                "--fingerprint", "fp1",
                "windowSize", "5", "deviations", "3", "minDataPoints", "3",
                "direction", "BOTH"});
        commands.add(new String[]{"node", "list"});
        for (Path f : uploadFiles) {
            commands.add(new String[]{"run", "upload", f.toString()});
        }
        commands.add(new String[]{"folder", "values"});
        commands.add(new String[]{"cd", ".."});

        List<String> results = run(aeshLauncher, commands.toArray(new String[0][]));

        String output = results.get(results.size() - 2);

        // The node list is the second "list nodes" command (index 7, after add stddev)
        String nodeList = results.get(7);
        assertTrue(nodeList.contains("sdNode"), "Node list should contain sdNode\n" + nodeList);

        // After uploading 5 stable + 1 anomaly, the anomaly should be detected
        // Output should contain stddev detection fields
        assertTrue(output.contains("\"mean\""), "Detection should contain mean\n" + output);
        assertTrue(output.contains("\"stddev\""), "Detection should contain stddev\n" + output);
        assertTrue(output.contains("\"deviations\""), "Detection should contain deviations\n" + output);
        assertTrue(output.contains("\"direction\""), "Detection should contain direction\n" + output);
         assertTrue(output.contains("above"), "200 should be detected as above the threshold\n" + output);
    }

    // --- Folder context (cd) tests ---
    // These tests simulate how a user would actually use the CLI:
    // cd into a folder, then operate without specifying --to/--from explicitly.

    @Test
    public void cd_add_nodes_upload_list_values() throws IOException {
        String testName = "cd_add_nodes_upload_list_values";
        Path folder = Files.createTempDirectory("h5m");
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                { "cpu": 42, "mem": "8gb" }
                """
        );
        // Create folder, cd into it, then add nodes and upload without --to/--from
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "cpu", ".cpu"},
                new String[]{"node", "add", "jq", "mem", ".mem"},
                new String[]{"node", "list"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values"}
        );

        String nodeList = results.get(4);
        assertTrue(nodeList.contains("cpu"), "node list should show cpu node\n" + nodeList);
        assertTrue(nodeList.contains("mem"), "node list should show mem node\n" + nodeList);

        String values = results.getLast();
        assertTrue(values.contains("Count: 2"), "expect 2 values after upload\n" + values);
    }

    @Test
    public void cd_add_nodes_remove_node() {
        String testName = "cd_add_nodes_remove_node";
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "foo", ".foo"},
                new String[]{"node", "add", "jq", "bar", ".bar"},
                new String[]{"node", "list"},
                new String[]{"node", "remove", "bar"},
                new String[]{"node", "list"}
        );

        String beforeRemove = results.get(4);
        assertTrue(beforeRemove.contains("bar"), "node list should show bar before remove\n" + beforeRemove);

        String afterRemove = results.getLast();
        assertTrue(afterRemove.contains("foo"), "node list should still show foo\n" + afterRemove);
        assertFalse(afterRemove.contains("bar"), "node list should not show bar after remove\n" + afterRemove);
    }

    @Test
    public void cd_fixedthreshold_with_upload() throws IOException {
        String testName = "cd_fixedthreshold_with_upload";
        Path folder = Files.createTempDirectory("h5m");
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                { "y": 5.0, "fp1": "alpha" }
                """
        );
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                { "y": 50.0, "fp1": "alpha" }
                """
        );
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                { "y": 150.0, "fp1": "alpha" }
                """
        );

        // cd into folder, set up nodes and detection, upload — all without --to/--from
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "rangeNode", ".y"},
                new String[]{"node", "add", "jq", "fp1", ".fp1"},
                new String[]{"node", "add", "fixedthreshold", "ftNode", "--range", "rangeNode", "--fingerprint", "fp1", "--min", "10", "--max", "100"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values"}
        );

        String values = results.getLast();
        // 3 rangeNode + 3 fp1 + 3 _fp-ftNode + 2 violations = 11
        assertTrue(values.contains("Count: 11"), "expect 11 values\n" + values);
    }

    @Test
    public void cd_then_cd_back() {
        // Verify cd .. clears the folder context
        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", "myFolder"},
                new String[]{"cd", "myFolder"},
                new String[]{"node", "add", "jq", "foo", ".foo"},
                new String[]{"node", "list"},
                new String[]{"cd", ".."},
                new String[]{"node", "list"}
        );

        String inFolder = results.get(3);
        assertTrue(inFolder.contains("foo"), "node list in folder should show foo\n" + inFolder);

        String outOfFolder = results.getLast();
        // After cd .., node list without --from should fail or show error
        assertTrue(outOfFolder.contains("folder name is required") || outOfFolder.contains("not found"),
                "node list without context should require --from\n" + outOfFolder);
    }

    @Test
    public void cd_structure_and_recalculate() throws IOException {
        String testName = "cd_structure_and_recalculate";
        Path folder = Files.createTempDirectory("h5m");
        Files.writeString(Files.createTempFile(folder, "h5m", ".json").toAbsolutePath(),
                """
                { "cpu": 4, "mem": "16gb" }
                """
        );

        List<String> results = run(aeshLauncher,
                new String[]{"folder", "add", testName},
                new String[]{"cd", testName},
                new String[]{"node", "add", "jq", "cpu", ".cpu"},
                new String[]{"node", "add", "jq", "mem", ".mem"},
                new String[]{"run", "upload", folder.toString()},
                new String[]{"folder", "values"},
                new String[]{"folder", "structure"},
                new String[]{"folder", "recalculate"},
                new String[]{"folder", "values"}
        );

        String values1 = results.get(5);
        assertTrue(values1.contains("Count: 2"), "expect 2 values after upload\n" + values1);

        String structure = results.get(6);
        assertTrue(structure.contains("cpu"), "structure should show cpu node\n" + structure);

        String values2 = results.getLast();
        assertTrue(values2.contains("Count: 2"), "expect 2 values after recalculate\n" + values2);
    }

}
