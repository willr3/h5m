package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.api.Folder;
import io.hyperfoil.tools.h5m.entity.FolderEntity;
import io.hyperfoil.tools.h5m.svc.FolderService;
import jakarta.inject.Inject;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name="load-legacy-runs")
public class LoadLegacyRuns implements Callable<Integer> {

    @Inject
    FolderService folderService;

    @CommandLine.Option(names = {"username"}, description = "legacy db username", defaultValue = "quarkus") String username;
    @CommandLine.Option(names = {"password"}, description = "legacy db password", defaultValue = "quarkus") String password;
    @CommandLine.Option(names = {"url"}, description = "legacy connection url",defaultValue = "jdbc:postgresql://0.0.0.0:") String url;
    @CommandLine.Option(names = {"testId"}, description = "specify which test to load. Loads all if unspecified" ) Long testId;
    @CommandLine.Option(names = {"limit"}, description = "max runs to load", defaultValue = "-1") int limit;

    @Override
    public Integer call() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "1");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL,username); //username
        props.put(AgroalPropertiesReader.CREDENTIAL,password);//password
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.postgresql.Driver");
        props.put(AgroalPropertiesReader.JDBC_URL, url );
        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());

        Map<Long,String> tests = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try(Connection connection = ds.getConnection()){
            if(testId!=null && testId > -1){
                try(PreparedStatement statement = connection.prepareStatement("select name from test where id = ?")){
                    statement.setLong(1, testId);
                    try (ResultSet rs = statement.executeQuery()){
                        while(rs.next()){
                            tests.put(testId, rs.getString("name"));
                        }
                    }
                }
            }else {
                try (Statement statement = connection.createStatement()) {
                    try (ResultSet rs = statement.executeQuery("select id,name from test")) {
                        while (rs.next()) {
                            tests.put(rs.getLong(1), rs.getString(2));
                        }
                    }
                }
            }
            System.out.println("loaded "+tests.size()+" legacy tests");
            for(Long testId : tests.keySet()){
                String name = tests.get(testId);
                Folder folder = folderService.byName(name);
                if(folder == null){
                    System.out.println("Failed to find Folder for test "+name+" id="+testId);
                    continue;
                }
                try (PreparedStatement ps = connection.prepareStatement("select count(id) from run where testid = ? and trashed = false")) {
                    ps.setLong(1, testId);
                    try (ResultSet rs = ps.executeQuery()){
                        while(rs.next()){
                            System.out.println("loading "+rs.getLong(1)+" uploads to "+name);
                        }
                    }
                }
                String runQuery = limit > 0
                        ? "select id,data from run where testid = ? and trashed = false order by id desc limit ?"
                        : "select id,data from run where testid = ? and trashed = false order by id desc";
                connection.setAutoCommit(false);
                try (PreparedStatement ps = connection.prepareStatement(runQuery)) {
                    ps.setFetchSize(5);
                    ps.setLong(1, testId);
                    if (limit > 0) ps.setInt(2, limit);
                    int count = 0;
                    try (ResultSet rs = ps.executeQuery()) {
                        while(rs.next()){
                            Long id = rs.getLong(1);
                            System.out.println(name+" "+id);
                            JsonNode data = mapper.readTree(rs.getString(2));
                            folderService.upload(folder.name(),null,data);
                            count++;
                        }
                    }
                    System.out.println("loaded " + count + " runs");
                } finally {
                    connection.setAutoCommit(true);
                }
            }
        } finally {
            ds.close();
        }
        return 0;
    }
}
