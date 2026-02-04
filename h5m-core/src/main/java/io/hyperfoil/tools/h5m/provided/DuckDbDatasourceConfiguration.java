package io.hyperfoil.tools.h5m.provided;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.hyperfoil.tools.yaup.AsciiArt;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DuckDbDatasourceConfiguration {

    
    @Alternative
    @Produces
    @ApplicationScoped
    //@Priority(9999)
    //@Default
    @Priority(8888)
    @Named("duckdb")
    public AgroalDataSource initDatasource(/*CommandLine.ParseResult parseResult*/) throws SQLException {
        Map<String, String> props = new HashMap<>();
        props.put(AgroalPropertiesReader.MAX_SIZE, "10");
        props.put(AgroalPropertiesReader.MIN_SIZE, "1");
        props.put(AgroalPropertiesReader.INITIAL_SIZE, "1");
        props.put(AgroalPropertiesReader.MAX_LIFETIME_S, "57");
        props.put(AgroalPropertiesReader.ACQUISITION_TIMEOUT_S, "54");
        props.put(AgroalPropertiesReader.JDBC_URL, "jdbc:duckdb:/tmp/h5m-duck.db");
        props.put(AgroalPropertiesReader.PROVIDER_CLASS_NAME , "org.duckdb.DuckDBDriver");
        AgroalDataSource ds  = AgroalDataSource.from(new AgroalPropertiesReader()
                .readProperties(props)
                .get());
//        try(Connection connection = ds.getConnection()) {
//            try(Statement statement = connection.createStatement()) {
//                statement.execute("CREATE TABLE test (x INTEGER, y INTEGER, z INTEGER)");
//            }
//        }
/*
        try(Connection connection = ds.getConnection()){
            initDb(connection);
        }
*/
        return ds;
    }


}
