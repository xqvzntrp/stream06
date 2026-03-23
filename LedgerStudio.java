import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final String DEFAULT_DATABASE = System.getProperty("user.name", "postgres");
    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/" + DEFAULT_DATABASE;
    private static final String DEFAULT_DB_USER = System.getProperty("user.name", "postgres");

    private LedgerStudio() {
    }

    public static void main(String[] args) {
        new StudioApp().run();
    }

    private static final class StudioApp {
        private static final String THREAD_SUMMARY_SQL =
                "select s.stream_code, o.object_kind, o.object_key, ts.thread_id, ts.thread_status, ts.opened_ts, ts.closed_ts " +
                "from api_ledger.v_thread_status ts " +
                "join api_ledger.api_stream s on s.stream_id = ts.stream_id " +
                "join api_ledger.api_object o on o.object_id = ts.object_id " +
                "order by ts.thread_id";

        private static final String OPEN_THREAD_SUMMARY_SQL =
                "select s.stream_code, o.object_kind, o.object_key, ts.thread_id, ts.thread_status, ts.opened_ts, ts.closed_ts " +
                "from api_ledger.v_thread_status ts " +
                "join api_ledger.api_stream s on s.stream_id = ts.stream_id " +
                "join api_ledger.api_object o on o.object_id = ts.object_id " +
                "where ts.thread_status = 'OPEN' " +
                "order by ts.thread_id";

        private static final String GOVERNING_SNAPSHOT_SQL =
                "select s.stream_code, ro.object_kind, ro.object_key, ro.object_name, ro.governing_thread_id " +
                "from api_ledger.v_registry_object ro " +
                "join api_ledger.api_stream s on s.stream_id = ro.stream_id " +
                "order by ro.object_kind, ro.object_key";

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
                    if (!handleCommand(line)) {
                        return;
                    }
                } catch (Exception e) {
                    System.out.println("ERROR: " + summarizeThrowable(e));
                }
            }
        }

        private boolean handleCommand(String line) throws Exception {
            if (matches(line, "quit") || matches(line, "exit")) {
                shutdown();
                return false;
            }
            if (matches(line, "help")) {
                printHelp();
                return true;
            }
            if (matches(line, "connect")) {
                handleConnect();
                return true;
            }
            if (matches(line, "disconnect")) {
                handleDisconnect();
                return true;
            }
            if (matches(line, "status")) {
                handleStatus();
                return true;
            }
            if (matches(line, "streams")) {
                requireConnection();
                printQuery(
                        "select stream_id, stream_code, stream_title, created_ts " +
                        "from api_ledger.api_stream " +
                        "order by stream_code"
                );
                return true;
            }
            if (matches(line, "participants")) {
                requireConnection();
                printQuery(
                        "select participant_id, participant_code, display_name, created_ts " +
                        "from api_ledger.api_participant " +
                        "order by participant_code"
                );
                return true;
            }
            if (matches(line, "objects")) {
                requireConnection();
                printQuery(
                        "select object_id, stream_id, object_kind, object_key, object_name, created_ts " +
                        "from api_ledger.api_object " +
                        "order by object_kind, object_key"
                );
                return true;
            }
            if (matches(line, "threads")) {
                requireConnection();
                printQuery(THREAD_SUMMARY_SQL);
                return true;
            }
            if (matches(line, "open-threads")) {
                requireConnection();
                printQuery(OPEN_THREAD_SUMMARY_SQL);
                return true;
            }
            if (matches(line, "governance") || matches(line, "snapshot-objects")) {
                requireConnection();
                printQuery(GOVERNING_SNAPSHOT_SQL);
                return true;
            }
            if (matches(line, "snapshot-relations")) {
                requireConnection();
                printQuery(
                        "select stream_id, relation_id, source_object_id, relation_type, target_object_id, ordinal_no, thread_id " +
                        "from api_ledger.v_registry_relation " +
                        "order by relation_id"
                );
                return true;
            }
            if (matches(line, "diagnostics")) {
                requireConnection();
                printDiagnostics();
                return true;
            }
            if (matches(line, "verify")) {
                requireConnection();
                handleVerify();
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("run-scenario ")) {
                requireConnection();
                handleRunScenario(line);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("run-file ")) {
                requireConnection();
                String filePath = line.substring("run-file ".length()).trim();
                runSqlFile(filePath);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("sql ")) {
                requireConnection();
                String sql = line.substring(4).trim();
                runAdHocSql(sql);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("commit ")) {
                requireConnection();
                handleRecordCommit(line);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("fulfill ")) {
                requireConnection();
                handleRecordFulfill(line);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("restart ")) {
                requireConnection();
                handleRecordRestart(line);
                return true;
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("relate ")) {
                throw new UnsupportedOperationException("The relate command is not installed in this repo yet.");
            }
            if (line.toLowerCase(Locale.ROOT).startsWith("supersede ")) {
                throw new UnsupportedOperationException("The supersede command is not installed in this repo yet.");
            }

            System.out.println("Unknown command. Type 'help'.");
            return true;
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
            System.out.println("run-scenario <scenario-name>");
            System.out.println("verify");
            System.out.println("run-file <path-to-sql-file>");
            System.out.println("sql <select-or-other-sql>");
            System.out.println();
            System.out.println("Procedure wrappers");
            System.out.println("------------------");
            System.out.println("commit <stream_code> <participant_code> <object_kind> <object_key>");
            System.out.println("fulfill <stream_code> <participant_code> <thread_id>");
            System.out.println("restart <stream_code> <participant_code> <thread_id>");
            System.out.println();
            System.out.println("exit");
            System.out.println("quit");
        }

        private void handleConnect() throws SQLException {
            if (connection != null && !connection.isClosed()) {
                System.out.println("Already connected.");
                return;
            }

            System.out.print("JDBC URL [" + DEFAULT_JDBC_URL + "]: ");
            String url = scanner.nextLine().trim();
            if (url.isEmpty()) {
                url = DEFAULT_JDBC_URL;
            }

            System.out.print("User [" + DEFAULT_DB_USER + "]: ");
            String user = scanner.nextLine().trim();
            if (user.isEmpty()) {
                user = DEFAULT_DB_USER;
            }

            System.out.print("Password: ");
            String password = scanner.nextLine();

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(true);
            setSearchPath();

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
                    "from api_ledger.v_ambiguous_object " +
                    "order by object_id"
            );

            System.out.println("\nObjects with open threads:");
            printQuery(
                    "select stream_id, object_id, count(*) as open_thread_count " +
                    "from api_ledger.v_thread_status " +
                    "where thread_status = 'OPEN' " +
                    "group by stream_id, object_id " +
                    "order by object_id"
            );

            System.out.println("\nHistorical accepted objects:");
            printQuery(
                    "select a.stream_id, a.object_id, a.thread_id " +
                    "from api_ledger.v_accepted_thread a " +
                    "left join api_ledger.v_governing_thread g on g.thread_id = a.thread_id " +
                    "where g.thread_id is null " +
                    "order by a.object_id, a.thread_id"
            );
        }

        private void handleRunScenario(String line) throws Exception {
            String[] parts = splitArgs(line, 2);
            Path scenarioPath = resolveScenarioPath(parts[1]);
            String sql = Files.readString(scenarioPath);
            boolean previousAutoCommit = connection.getAutoCommit();

            try {
                connection.setAutoCommit(false);
                setSearchPath();
                executeSqlScript(sql);

                System.out.println("Scenario executed: " + scenarioPath.getFileName());
                System.out.println("\nLifecycle summary:");
                printQuery(THREAD_SUMMARY_SQL);
                System.out.println("\nGoverning snapshot:");
                printQuery(GOVERNING_SNAPSHOT_SQL);

                connection.commit();
                System.out.println("\nScenario committed.");
            } catch (Exception e) {
                rollbackQuietly();
                throw new IllegalStateException("Scenario rolled back: " + summarizeThrowable(e), e);
            } finally {
                connection.setAutoCommit(previousAutoCommit);
                setSearchPath();
            }
        }

        private void handleVerify() throws Exception {
            Path verifyPath = Path.of("ledger_verify.sql");
            String sql = Files.readString(verifyPath);
            boolean previousAutoCommit = connection.getAutoCommit();

            try {
                connection.setAutoCommit(false);
                setSearchPath();
                executeSqlScript(sql);
                connection.commit();
                System.out.println("PASS: " + verifyPath.getFileName());
            } catch (Exception e) {
                rollbackQuietly();
                System.out.println("FAIL: " + summarizeThrowable(e));
            } finally {
                connection.setAutoCommit(previousAutoCommit);
                setSearchPath();
            }
        }

        private Path resolveScenarioPath(String scenarioName) {
            Path directPath = Path.of(scenarioName);
            if (Files.isRegularFile(directPath)) {
                return directPath;
            }

            String normalized = scenarioName.toLowerCase(Locale.ROOT);
            if (normalized.endsWith(".sql")) {
                Path sqlPath = Path.of(normalized);
                if (Files.isRegularFile(sqlPath)) {
                    return sqlPath;
                }
            }

            Path prefixedPath = Path.of("ledger_" + normalized + ".sql");
            if (Files.isRegularFile(prefixedPath)) {
                return prefixedPath;
            }

            throw new IllegalArgumentException("Scenario file not found for: " + scenarioName);
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
                    "from api_ledger.record_fulfill(?, ?, ?)"
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
                    "from api_ledger.record_restart(?, ?, ?)"
            )) {
                ps.setString(1, streamCode);
                ps.setString(2, participantCode);
                ps.setLong(3, threadId);

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
            executeSqlScript(Files.readString(Path.of(filePath)));
            System.out.println("Executed file: " + filePath);
        }

        private void executeSqlScript(String sql) throws SQLException {
            try (Statement st = connection.createStatement()) {
                st.execute(sql);
                consumeRemainingResults(st);
            }
        }

        private void consumeRemainingResults(Statement st) throws SQLException {
            while (true) {
                ResultSet rs = st.getResultSet();
                if (rs != null) {
                    rs.close();
                } else if (st.getUpdateCount() == -1) {
                    break;
                }

                if (!st.getMoreResults() && st.getUpdateCount() == -1) {
                    break;
                }
            }
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

        private void setSearchPath() throws SQLException {
            try (Statement st = connection.createStatement()) {
                st.execute("set search_path = " + DEFAULT_SCHEMA);
            }
        }

        private void rollbackQuietly() {
            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                System.out.println("Rollback warning: " + rollbackError.getMessage());
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

        private String summarizeThrowable(Throwable throwable) {
            StringBuilder builder = new StringBuilder();
            Throwable current = throwable;

            while (current != null) {
                String message = current.getMessage();
                if (message != null && !message.isBlank()) {
                    if (builder.length() > 0) {
                        builder.append(" | ");
                    }
                    builder.append(message.replace('\n', ' '));
                }
                current = current.getCause();
            }

            if (builder.length() == 0) {
                return throwable.getClass().getSimpleName();
            }
            return builder.toString();
        }

        private void shutdown() {
            handleDisconnect();
            scanner.close();
            System.out.println("Goodbye.");
        }
    }
}
