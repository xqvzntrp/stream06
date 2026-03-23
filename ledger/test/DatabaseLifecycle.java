package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public final class DatabaseLifecycle {

    private final String adminUrl;
    private final String user;
    private final String password;

    public DatabaseLifecycle(String adminUrl, String user, String password) {
        this.adminUrl = adminUrl;
        this.user = user;
        this.password = password == null ? "" : password;
    }

    public TestDatabase createFreshDatabase() throws SQLException {
        String dbName = "ledger_test_" + UUID.randomUUID().toString().replace("-", "");

        try (Connection cx = DriverManager.getConnection(adminUrl, user, password);
             Statement st = cx.createStatement()) {
            st.execute("create database " + quoteIdentifier(dbName));
        }

        String dbUrl = buildDatabaseUrl(adminUrl, dbName);
        Connection cx = DriverManager.getConnection(dbUrl, user, password);
        cx.setAutoCommit(true);

        return new TestDatabase(dbName, dbUrl, cx);
    }

    public void dropDatabase(String dbName) throws SQLException {
        validateDatabaseName(dbName);

        try (Connection cx = DriverManager.getConnection(adminUrl, user, password);
             Statement st = cx.createStatement()) {
            st.execute(
                    "select pg_terminate_backend(pid) " +
                    "from pg_stat_activity " +
                    "where datname = '" + escapeLiteral(dbName) + "' " +
                    "and pid <> pg_backend_pid()"
            );
            st.execute("drop database if exists " + quoteIdentifier(dbName));
        }
    }

    private static String buildDatabaseUrl(String jdbcUrl, String dbName) {
        int slash = jdbcUrl.lastIndexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("Admin JDBC URL must end with a database name: " + jdbcUrl);
        }
        return jdbcUrl.substring(0, slash + 1) + dbName;
    }

    private static void validateDatabaseName(String dbName) {
        if (!dbName.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Unsafe database name: " + dbName);
        }
    }

    private static String quoteIdentifier(String value) {
        validateDatabaseName(value);
        return '"' + value + '"';
    }

    private static String escapeLiteral(String value) {
        return value.replace("'", "''");
    }

    public static final class TestDatabase {
        public final String name;
        public final String url;
        public final Connection connection;

        TestDatabase(String name, String url, Connection connection) {
            this.name = name;
            this.url = url;
            this.connection = connection;
        }
    }
}
