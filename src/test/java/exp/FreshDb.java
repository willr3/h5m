package exp;

import exp.provided.DatasourceConfiguration;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;


public class FreshDb {

    @Inject
    AgroalDataSource ds;

    @ConfigProperty(name="quarkus.datasource.db-kind")
    String dbKind;

    @BeforeEach
    @AfterEach
    public void dropRows() throws SQLException {
        try(Connection conn = ds.getConnection()){
            conn.setAutoCommit(true);
            try(Statement stmt = conn.createStatement()){

                if(dbKind.equals("postgresql")){
                    stmt.executeUpdate("TRUNCATE TABLE work_values CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE work_nodes CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE work CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE value_edge CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE value CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE folder CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE nodegroup CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE node_edge CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE node CASCADE");
                }else if (dbKind.equals("sqlite")){
                    stmt.executeUpdate("DELETE from work_values");
                    stmt.executeUpdate("DELETE from work_nodes");
                    stmt.executeUpdate("DELETE from work");
                    stmt.executeUpdate("DELETE from value_edge");
                    stmt.executeUpdate("DELETE from value");
                    stmt.executeUpdate("DELETE from folder");
                    stmt.executeUpdate("DELETE from nodegroup");
                    stmt.executeUpdate("DELETE from node_edge");
                    stmt.executeUpdate("DELETE from node");
                }
            }
        }
    }
}
