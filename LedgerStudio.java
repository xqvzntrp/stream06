import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Scanner;

/**
 * LedgerStudio
 *
 * Minimal standalone operator shell for the collaboration ledger runtime.
 *
 * Design rule:
 * - Java does NOT interpret protocol meaning.
 * - Java only invokes SQL procedures and reads SQL views.
 *
 * Expected PostgreSQL runtime objects:
 * - schema api_ledger
 * - tables, views, and functions already installed
 *
 * Suggested usage:
 *
 *   javac LedgerStudio.java
 *   java -cp .:postgresql-42.7.3.jar LedgerStudio
 *
 * Windows:
 *   java -cp .;postgresql-42.7.3.jar LedgerStudio
 */
public final class LedgerStudio {

    private static final String DEFAULT_SCHEMA = "api_ledger";

    private LedgerStudio() {
    }

    public static void main(String[] args) {
        new StudioApp().run();
    }

    private static final class StudioApp {
        private final Scanner scanner = new Scanner(System.in);
        private Connection connection;

        void run() {
            printBanner();
            printHelp();

            while (true) {
                System.out.print("\nstudio> ");
                if (!scanner.hasNextLine()) {
                    shutdown();
                    return;
                }

                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }

                try {
                    if (matches(line, "quit") || matches(line, "exit")) {
                        shutdown();
                        return;
                    } else if (matches(line, "help")) {
                        printHelp();
                    } else if (matches(line, "connect")) {
                        handleConnect();
                    } else if (matches(line, "disconnect")) {
                        handleDisconnect();
                    } else if (matches(line, "status")) {
                        handleStatus();
                    } else if (matches(line, "streams")) {
                        requireConnection();
                        printQuery(
                                "select stream_id, stream_code, stream_title, created_ts " +
                                "from api_ledger.api_stream " +
                                "order by stream_code"
                        );
                    } else if (matches(line, "participants")) {
                        requireConnection();
                        printQuery(
                                "select participant_id, participant_code, display_name, created_ts " +
                                "from api_ledger.api_participant " +
                                "order by participant_code"
                        );
                    } else if (matches(line, "objects")) {
                        requireConnection();
                        printQuery(
                                "select object_id, stream_id, object_kind, object_key, object_name, created_ts " +
                                "from api_ledger.api_object " +
                                "order by object_kind, object_key"
                        );
                    } else if (matches(line, "threads")) {
                        requireConnection();
                        printQuery(
                                "select stream_code, object_kind, object_key, thread_id, derived_thread_status, opened_ts, closed_ts " +
                                "from api_ledger.v_api_thread_inspect " +
                                "order by thread_id"
                        );
                    } else if (matches(line, "open-threads")) {
                        requireConnection();
                        printQuery(
                                "select stream_code, object_kind, object_key, thread_id, derived_thread_status, opened_ts, closed_ts " +
                                "from api_ledger.v_api_thread_inspect " +
                                "where derived_thread_status = 'OPEN' " +
                                "order by thread_id"
                        );
                    } else if (matches(line, "governance")) {
                        requireConnection();
                        printQuery(
                                "select stream_code, object_kind, object_key, governing_thread_id " +
                                "from api_ledger.v_api_governance_inspect " +
                                "order by object_kind, object_key"
                        );
                    } else if (matches(line, "snapshot-objects")) {
                        requireConnection();
                        printQuery(
                                "select stream_id, object_id, object_kind, object_key, object_name, governing_thread_id " +
                                "from api_ledger.v_snapshot_object " +
                                "order by object_kind, object_key"
                        );
                    } else if (matches(line, "snapshot-relations")) {
                        requireConnection();
                        printQuery(
                                "select stream_id, relation_id, source_object_id, relation_type, target_object_id, ordinal_no, governing_thread_id " +
                                "from api_ledger.v_snapshot_relation " +
                                "order by relation_id"
                        );
                    } else if (matches(line, "diagnostics")) {
                        requireConnection();
                        printDiagnostics();
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("run-file ")) {
                        requireConnection();
                        String filePath = line.substring("run-file ".length()).trim();
                        runSqlFile(filePath);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("sql ")) {
                        requireConnection();
                        String sql = line.substring(4).trim();
                        runAdHocSql(sql);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("commit ")) {
                        requireConnection();
                        handleRecordCommit(line);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("fulfill ")) {
                        requireConnection();
                        handleRecordFulfill(line);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("restart ")) {
                        requireConnection();
                        handleRecordRestart(line);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("relate ")) {
                        requireConnection();
                        handleRecordRelation(line);
                    } else if (line.toLowerCase(Locale.ROOT).startsWith("supersede ")) {
                        requireConnection();
                        handleRecordSupersede(line);
                    } else {
                        System.out.println("Unknown command. Type 'help'.");
                    }
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }

        private void printBanner() {
            System.out.println("========================================");
            System.out.println("LedgerStudio");
            System.out.println("Minimal operator shell for api_ledger");
            System.out.println("========================================");
        }

        private void printHelp() {
            System.out.println();
            System.out.println("Commands");
            System.out.println("--------");
            System.out.println("help");
            System.out.println("connect");
            System.out.println("disconnect");
            System.out.println("status");
            System.out.println("streams");
            System.out.println("participants");
            System.out.println("objects");
            System.out.println("threads");
            System.out.println("open-threads");
            System.out.println("governance");
            System.out.println("snapshot-objects");
            System.out.println("snapshot-relations");
            System.out.println("diagnostics");
            System.out.println("run-file <path-to-sql-file>");
            System.out.println("sql <select-or-other-sql>");
            System.out.println();
            System.out.println("Procedure wrappers");
            System.out.println("------------------");
            System.out.println("commit <stream_code> <participant_code> <object_kind> <object_key>");
            System.out.println("fulfill <stream_code> <participant_code> <thread_id>");
            System.out.println("restart <stream_code> <participant_code> <thread_id>");
            System.out.println("relate <stream_code> <participant_code> <thread_id> <source_kind> <source_key> <relation_type> <target_kind> <target_key> [ordinal_no]");
            System.out.println("supersede <stream_code> <participant_code> <superseding_thread_id> <superseded_thread_id>");
            System.out.println();
            System.out.println("exit");
            System.out.println("quit");
        }

        private void handleConnect() throws SQLException {
            if (connection != null && !connection.isClosed()) {
                System.out.println("Already connected.");
                return;
            }

            System.out.print("JDBC URL [jdbc:postgresql://localhost:5432/postgres]: ");
            String url = scanner.nextLine().trim();
            if (url.isEmpty()) {
                url = "jdbc:postgresql://localhost:5432/postgres";
            }

            System.out.print("User [postgres]: ");
            String user = scanner.nextLine().trim();
            if (user.isEmpty()) {
                user = "postgres";
            }

            System.out.print("Password: ");
            String password = scanner.nextLine();

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);

            try (Statement st = connection.createStatement()) {
                st.execute("set search_path = " + DEFAULT_SCHEMA);
            }

            System.out.println("Connected.");
        }

        private void handleDisconnect() {
            if (connection == null) {
                System.out.println("Not connected.");
                return;
            }

            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("Disconnect warning: " + e.getMessage());
            } finally {
                connection = null;
            }

            System.out.println("Disconnected.");
        }

        private void handleStatus() throws SQLException {
            if (connection == null || connection.isClosed()) {
                System.out.println("Status: not connected");
                return;
            }

            System.out.println("Status: connected");

            try (Statement st = connection.createStatement();
                 ResultSet rs = st.executeQuery("select current_database(), current_schema(), now()")) {
                if (rs.next()) {
                    System.out.println("Database: " + rs.getString(1));
                    System.out.println("Schema:   " + rs.getString(2));
                    System.out.println("Now:      " + rs.getString(3));
                }
            }
        }

        private void printDiagnostics() throws SQLException {
            System.out.println("\nAmbiguous governing objects:");
            printQuery(
                    "select stream_id, object_id, governing_thread_count " +
                    "from api_ledger.v_snapshot_ambiguous_object " +
                    "order by object_id"
            );

            System.out.println("\nObjects with open threads:");
            printQuery(
                    "select stream_id, object_id, open_thread_count " +
                    "from api_ledger.v_snapshot_open_object " +
                    "order by object_id"
            );

            System.out.println("\nHistorical accepted objects:");
            printQuery(
                    "select stream_id, object_id, thread_id " +
                    "from api_ledger.v_snapshot_historical_accepted_object " +
                    "order by object_id, thread_id"
            );
        }

        private void handleRecordCommit(String line) throws SQLException {
            String[] parts = splitArgs(line, 5);
            String streamCode = parts[1];
            String participantCode = parts[2];
            String objectKind = parts[3];
            String objectKey = parts[4];

            try (PreparedStatement ps = connection.prepareStatement(
                    "select act_id, act_seq, thread_id " +
                    "from api_ledger.record_commit(?, ?, ?, ?, null)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setString(3, objectKind);
                ps.setString(4, objectKey);

                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }

        private void handleRecordFulfill(String line) throws SQLException {
            String[] parts = splitArgs(line, 4);
            String streamCode = parts[1];
            String participantCode = parts[2];
            long threadId = parseLong(parts[3], "thread_id");

            try (PreparedStatement ps = connection.prepareStatement(
                    "select act_id, act_seq " +
                    "from api_ledger.record_fulfill(?, ?, ?, null)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setLong(3, threadId);

                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }

        private void handleRecordRestart(String line) throws SQLException {
            String[] parts = splitArgs(line, 4);
            String streamCode = parts[1];
            String participantCode = parts[2];
            long threadId = parseLong(parts[3], "thread_id");

            try (PreparedStatement ps = connection.prepareStatement(
                    "select act_id, act_seq " +
                    "from api_ledger.record_restart(?, ?, ?, null)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setLong(3, threadId);

                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }

        private void handleRecordRelation(String line) throws SQLException {
            String[] parts = line.trim().split("\\s+");
            if (parts.length != 9 && parts.length != 10) {
                throw new IllegalArgumentException(
                        "Usage: relate <stream_code> <participant_code> <thread_id> <source_kind> <source_key> <relation_type> <target_kind> <target_key> [ordinal_no]"
                );
            }

            String streamCode = parts[1];
            String participantCode = parts[2];
            long threadId = parseLong(parts[3], "thread_id");
            String sourceKind = parts[4];
            String sourceKey = parts[5];
            String relationType = parts[6];
            String targetKind = parts[7];
            String targetKey = parts[8];
            Integer ordinalNo = (parts.length == 10) ? Integer.valueOf(parts[9]) : null;

            try (PreparedStatement ps = connection.prepareStatement(
                    "select relation_id, created_by_act_id, act_seq " +
                    "from api_ledger.record_relation(?, ?, ?, ?, ?, ?, ?, ?, ?, null)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setLong(3, threadId);
                ps.setString(4, sourceKind);
                ps.setString(5, sourceKey);
                ps.setString(6, relationType);
                ps.setString(7, targetKind);
                ps.setString(8, targetKey);

                if (ordinalNo == null) {
                    ps.setNull(9, java.sql.Types.INTEGER);
                } else {
                    ps.setInt(9, ordinalNo);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }

        private void handleRecordSupersede(String line) throws SQLException {
            String[] parts = splitArgs(line, 5);
            String streamCode = parts[1];
            String participantCode = parts[2];
            long supersedingThreadId = parseLong(parts[3], "superseding_thread_id");
            long supersededThreadId = parseLong(parts[4], "superseded_thread_id");

            try (PreparedStatement ps = connection.prepareStatement(
                    "select act_id, act_seq, thread_supersession_id " +
                    "from api_ledger.record_supersede(?, ?, ?, ?, null)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setLong(3, supersedingThreadId);
                ps.setLong(4, supersededThreadId);

                try (ResultSet rs = ps.executeQuery()) {
                    printResultSet(rs);
                }
            }
        }

        private void runAdHocSql(String sql) throws SQLException {
            try (Statement st = connection.createStatement()) {
                boolean hasResultSet = st.execute(sql);

                if (hasResultSet) {
                    try (ResultSet rs = st.getResultSet()) {
                        printResultSet(rs);
                    }
                } else {
                    System.out.println("Update count: " + st.getUpdateCount());
                }
            }
        }

        private void runSqlFile(String filePath) throws Exception {
            String sql = java.nio.file.Files.readString(java.nio.file.Path.of(filePath));
            try (Statement st = connection.createStatement()) {
                st.execute(sql);
            }
            System.out.println("Executed file: " + filePath);
        }

        private void printQuery(String sql) throws SQLException {
            try (Statement st = connection.createStatement();
                 ResultSet rs = st.executeQuery(sql)) {
                printResultSet(rs);
            }
        }

        private void printResultSet(ResultSet rs) throws SQLException {
            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    System.out.print(" | ");
                }
                System.out.print(rs.getMetaData().getColumnLabel(i));
            }
            System.out.println();
            System.out.println("-".repeat(Math.max(10, columnCount * 16)));

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        System.out.print(" | ");
                    }
                    Object value = rs.getObject(i);
                    System.out.print(value == null ? "null" : value.toString());
                }
                System.out.println();
            }

            if (rowCount == 0) {
                System.out.println("(no rows)");
            }
        }

        private void requireConnection() {
            try {
                if (connection == null || connection.isClosed()) {
                    throw new IllegalStateException("Not connected. Use 'connect' first.");
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Connection status could not be checked: " + e.getMessage(), e);
            }
        }

        private boolean matches(String actual, String expected) {
            return actual.equalsIgnoreCase(expected);
        }

        private String[] splitArgs(String line, int expectedParts) {
            String[] parts = line.trim().split("\\s+");
            if (parts.length != expectedParts) {
                throw new IllegalArgumentException("Invalid command format. Type 'help'.");
            }
            return parts;
        }

        private long parseLong(String value, String label) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid " + label + ": " + value);
            }
        }

        private void shutdown() {
            handleDisconnect();
            scanner.close();
            System.out.println("Goodbye.");
        }
    }
}
