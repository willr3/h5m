package io.hyperfoil.tools.h5m;

import io.agroal.api.AgroalDataSource;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


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
                    stmt.executeUpdate("TRUNCATE TABLE upload_processing CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE folder_view_component CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE folder_view CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE notification_log CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE notification_config CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE api_key CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE team_members CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE value_edge CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE value CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE folder CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE node_group CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE node_edge CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE node CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE h5m_user CASCADE");
                    stmt.executeUpdate("TRUNCATE TABLE team CASCADE");
                }else if (dbKind.equals("sqlite")){
                    stmt.executeUpdate("DELETE from upload_processing");
                    stmt.executeUpdate("DELETE from folder_view_component");
                    stmt.executeUpdate("DELETE from folder_view");
                    stmt.executeUpdate("DELETE from notification_log");
                    stmt.executeUpdate("DELETE from notification_config");
                    stmt.executeUpdate("DELETE from api_key");
                    stmt.executeUpdate("DELETE from team_members");
                    stmt.executeUpdate("DELETE from value_edge");
                    stmt.executeUpdate("DELETE from value");
                    stmt.executeUpdate("DELETE from folder");
                    stmt.executeUpdate("DELETE from node_group");
                    stmt.executeUpdate("DELETE from node_edge");
                    stmt.executeUpdate("DELETE from node");
                    stmt.executeUpdate("DELETE from h5m_user");
                    stmt.executeUpdate("DELETE from team");
                }
            }
        }
    }

    protected long countRows(String table) throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return ((Number) rs.getObject(1)).longValue();
        }
    }
}
