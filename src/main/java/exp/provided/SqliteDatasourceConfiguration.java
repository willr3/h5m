package exp.provided;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.yaup.AsciiArt;
import io.quarkus.arc.DefaultBean;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

//https://github.com/quarkusio/quarkus/issues/7019

@ApplicationScoped
public class SqliteDatasourceConfiguration {

    @Inject @ConfigProperty(name = "h5m.foo",defaultValue = "1")
    int foo;

    public SqliteDatasourceConfiguration() {
    }

    public static String getPath(){
        String rtrn = "";
        if(System.getenv("H5M_PATH") !=null ){
            rtrn = System.getenv("H5M_PATH");
        } else {
            //check local directory
            Path currentRelativePath = Paths.get("");
            String s = currentRelativePath.toAbsolutePath().toString()+ File.separator+"h5m.db";
            if((new File(s).exists())){
                rtrn = s;
            }else{
                rtrn = System.getProperty("user.home")+File.separator+"h5m.db";
            }
        }
        return rtrn;
    }

    public void initDb(Connection connection){
        try (Statement statement = connection.createStatement()) {
            //tuning from https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
            statement.executeUpdate(
            """
            pragma journal_mode = WAL;
            pragma synchronous = normal;
            pragma temp_store = memory;
            """);
/*            try(ResultSet rs = statement.executeQuery("SELECT name FROM sqlite_master")){
                while(rs.next()){
                    System.out.println(rs.getObject(1));
                }
            }
*/
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Alternative
    @Produces
    @ApplicationScoped
    @Priority(9999)
    @Default
    @Named("default")
    public AgroalDataSource initDatasource(/*CommandLine.ParseResult parseResult*/) throws SQLException {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "10");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.JDBC_URL, "jdbc:sqlite:"+getPath());
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.sqlite.JDBC");
        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());
        try(Connection connection = ds.getConnection()){
            initDb(connection);
        }
        return ds;
    }


}
