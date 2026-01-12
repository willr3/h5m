package exp.provided;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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
public class DatasourceConfiguration {

    @ConfigProperty(name = "h5m.foo",defaultValue = "1")
    int foo;

    @ConfigProperty(name = "quarkus.datasource.db-kind",defaultValue = "sqlite")
    String dbKind;
    @ConfigProperty(name = "quarkus.datasource.jdbc.initial-size", defaultValue = "1")
    String initialSize;
    @ConfigProperty(name = "quarkus.datasource.jdbc.min-size", defaultValue = "1")
    String minSize;
    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "50")
    String maxSize;
    @ConfigProperty(name = "quarkus.profile")
    String profile;
    @ConfigProperty(name = "quarkus.datasource.username",defaultValue = "quarkus")
    String username;
    @ConfigProperty(name = "quarkus.datasource.password",defaultValue = "quarkus")
    String password;

    //defaultValue empty string does not work for sqlite
    @ConfigProperty(name="quarkus.datasource.jdbc.url",defaultValue = " ")
    String url;


    public DatasourceConfiguration() {
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
            -- pragma read_uncommitted = ON; -- for theorized 'dirty reads' in other connections
            """);
/*            try(ResultSet rs = statement.executeQuery("SELECT name FROM sqlite_master")){
                while(rs.next()){
                }
            }
*/
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeAgroalDataSource(@Disposes AgroalDataSource dataSource){
        if(dbKind.equals("postgresql")){
            dataSource.close();
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
        props.put(AgroalPropertiesReader.MAX_SIZE, maxSize);
        props.put(AgroalPropertiesReader.MIN_SIZE, minSize);
        props.put(AgroalPropertiesReader.INITIAL_SIZE, initialSize);
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.PRINCIPAL,username); //username
        props.put(AgroalPropertiesReader.CREDENTIAL,password);//password
        if("sqlite".equalsIgnoreCase(dbKind)){
            props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.sqlite.JDBC");
            if(!props.containsKey(AgroalPropertiesReader.JDBC_URL)){
                props.put(AgroalPropertiesReader.JDBC_URL, "jdbc:sqlite:"+getPath());
            }
        }else if ("postgresql".equalsIgnoreCase(dbKind)){
            props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.postgresql.Driver");
            props.put(AgroalPropertiesReader.JDBC_URL, url );
        }else{

        }


        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());
        if("sqlite".equalsIgnoreCase(dbKind)){
            try(Connection connection = ds.getConnection()){
                initDb(connection);
            }
        }
        return ds;
    }


}
