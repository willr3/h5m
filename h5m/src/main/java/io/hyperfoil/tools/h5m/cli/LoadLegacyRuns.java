package io.hyperfoil.tools.h5m.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.h5m.entity.Folder;
import io.hyperfoil.tools.h5m.svc.FolderService;
import jakarta.inject.Inject;
import org.apache.commons.compress.harmony.pack200.NewAttributeBands;
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
            try(Statement statement = connection.createStatement()){
                try(ResultSet rs = statement.executeQuery("select id,name from test")){
                    while(rs.next()){
                        tests.put(rs.getLong(1),rs.getString(2));
                    }
                }
            }
            System.out.println("loaded "+tests.size()+" legacy tests");

            tests.clear();
            tests.put(113L,"quarkus-spring-boot-comparison");
            for(Long testId : tests.keySet()){
                String name = tests.get(testId);
                Folder folder = folderService.byName(name);
                if(folder == null){
                    System.out.println("Failed to find Folder for test "+name+" id="+testId);
                    continue;
                }
                try (PreparedStatement ps = connection.prepareStatement("select id,data from run where testid = ? and trashed = false")) {
                    ps.setLong(1, testId);
                    try (ResultSet rs = ps.executeQuery()) {
                        while(rs.next()){
                            Long id = rs.getLong(1);
                            System.out.println(name+" "+id);
                            JsonNode data = mapper.readTree(rs.getString(2));
                            folderService.upload(folder,null,data);
                        }
                    }
                }
            }
        }finally {
            ds.close();
        }
        return 0;
    }
}
