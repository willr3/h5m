package io.hyperfoil.tools.h5m;

import io.agroal.api.AgroalDataSource;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManagerFactory;
import jakarta.transaction.Status;
import jakarta.transaction.TransactionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class FreshDb {

    @Inject
    AgroalDataSource ds;

    @Inject
    EntityManagerFactory emf;

    @Inject
    TransactionManager tm;

    @BeforeEach
    @AfterEach
    public void dropRows() throws Exception {
        // Evict 2LC before truncating tables — prevents stale cached entities
        emf.getCache().evictAll();

        if (tm.getStatus() != Status.STATUS_NO_TRANSACTION) {
            tm.rollback();
        }
        try(Connection conn = ds.getConnection()){
            try(Statement stmt = conn.createStatement()){
                stmt.executeUpdate("DELETE from processing_tracker");
                stmt.executeUpdate("DELETE from folder_view_component");
                stmt.executeUpdate("DELETE from folder_view");
                stmt.executeUpdate("DELETE from notification_log");
                stmt.executeUpdate("DELETE from notification_config");
                stmt.executeUpdate("DELETE from api_key");
                stmt.executeUpdate("DELETE from team_members");
                stmt.executeUpdate("DELETE from value_edge");
                stmt.executeUpdate("DELETE from value");
                stmt.executeUpdate("DELETE from folder");
                stmt.executeUpdate("UPDATE node SET group_id = NULL, original_group_id = NULL, target_group_id = NULL, previous_version_id = NULL, original_node_id = NULL");
                stmt.executeUpdate("DELETE from node_edge");
                stmt.executeUpdate("DELETE from node_group");
                stmt.executeUpdate("DELETE from node");
                stmt.executeUpdate("DELETE from h5m_user");
                stmt.executeUpdate("DELETE from team");
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
