package io.hyperfoil.tools.h5m.provided;

import io.agroal.api.AgroalPoolInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@ApplicationScoped
public class SqliteConnectionInitializer implements AgroalPoolInterceptor {

    @Override
    public void onConnectionCreate(Connection connection) {
        try {
            if ("SQLite".equalsIgnoreCase(connection.getMetaData().getDatabaseProductName())) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("pragma journal_mode=WAL");
                    stmt.execute("pragma synchronous=normal");
                    stmt.execute("pragma temp_store=memory");
                    stmt.execute("pragma busy_timeout=30000");
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to initialize SQLite connection", e);
                }
            }
        } catch (SQLException ignored) {
        }
    }
}
