import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * ObjectLocalAuthorityDemo
 *
 * Standalone database-backed demo of the object-local authority rule:
 * a responsibility thread may mutate only the object it was opened on.
 *
 * Usage:
 *   javac ObjectLocalAuthorityDemo.java
 *   LEDGER_DB_PASSWORD=alex java -cp .:postgresql-42.7.10.jar ObjectLocalAuthorityDemo
 */
public final class ObjectLocalAuthorityDemo {

    private static final String ENV_DB_DATABASE = "LEDGER_DB_DATABASE";
    private static final String ENV_DB_URL = "LEDGER_DB_URL";
    private static final String ENV_DB_USER = "LEDGER_DB_USER";
    private static final String ENV_DB_PASSWORD = "LEDGER_DB_PASSWORD";

    private static final String DEFAULT_DATABASE = firstNonBlank(
            System.getenv(ENV_DB_DATABASE),
            firstNonBlank(System.getenv("PGDATABASE"), System.getProperty("user.name", "postgres"))
    );
    private static final String DEFAULT_JDBC_URL = firstNonBlank(
            System.getenv(ENV_DB_URL),
            "jdbc:postgresql://localhost:5432/" + DEFAULT_DATABASE
    );
    private static final String DEFAULT_DB_USER = firstNonBlank(
            System.getenv(ENV_DB_USER),
            firstNonBlank(System.getenv("PGUSER"), System.getProperty("user.name", "postgres"))
    );
    private static final String DEFAULT_DB_PASSWORD = firstNonBlank(
            System.getenv(ENV_DB_PASSWORD),
            System.getenv("PGPASSWORD")
    );

    private static final String RESET_SQL =
            "truncate table " +
            "api_ledger.api_object_attr, " +
            "api_ledger.api_relation, " +
            "api_ledger.api_thread_supersession, " +
            "api_ledger.api_thread, " +
            "api_ledger.api_act, " +
            "api_ledger.api_object, " +
            "api_ledger.api_participant, " +
            "api_ledger.api_stream " +
            "restart identity cascade";

    private ObjectLocalAuthorityDemo() {
    }

    public static void main(String[] args) throws Exception {
        if (DEFAULT_DB_PASSWORD == null || DEFAULT_DB_PASSWORD.isBlank()) {
            throw new IllegalStateException("Set LEDGER_DB_PASSWORD or PGPASSWORD before running this demo.");
        }

        try (Connection connection = DriverManager.getConnection(DEFAULT_JDBC_URL, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD)) {
            connection.setAutoCommit(true);
            setSearchPath(connection);

            System.out.println("========================================");
            System.out.println("Object Local Authority Demo");
            System.out.println("========================================");
            System.out.println("JDBC URL: " + DEFAULT_JDBC_URL);
            System.out.println("User:     " + DEFAULT_DB_USER);

            resetLedger(connection);
            seedProtocolObjects(connection);

            System.out.println();
            System.out.println("Step 1: open an endpoint responsibility thread");
            long endpointThreadId = recordCommit(connection, "API", "ALICE", "ENDPOINT", "EP_HELLO");

            System.out.println();
            System.out.println("Step 2: the endpoint thread may define topology outward");
            recordRelate(connection, "API", "ALICE", endpointThreadId,
                    "ENDPOINT", "EP_HELLO", "HAS", "PARAMETER", "P_NAME", 1);

            System.out.println();
            System.out.println("Step 3: the endpoint thread may not mutate the parameter's state");
            expectSemanticFailure(
                    connection,
                    "attribute API ALICE " + endpointThreadId + " PARAMETER P_NAME DESCRIPTION Name TEXT",
                    () -> recordAttribute(connection, "API", "ALICE", endpointThreadId,
                            "PARAMETER", "P_NAME", "DESCRIPTION", "Name", "TEXT")
            );

            System.out.println();
            System.out.println("Step 4: the endpoint thread may still mutate the endpoint itself");
            recordAttribute(connection, "API", "ALICE", endpointThreadId,
                    "ENDPOINT", "EP_HELLO", "DESCRIPTION", "Greets the caller", "TEXT");
            recordFulfill(connection, "API", "ALICE", endpointThreadId);

            System.out.println();
            System.out.println("Step 5: open a separate parameter thread for parameter state");
            long parameterThreadId = recordCommit(connection, "API", "ALICE", "PARAMETER", "P_NAME");
            recordAttribute(connection, "API", "ALICE", parameterThreadId,
                    "PARAMETER", "P_NAME", "DESCRIPTION", "Name", "TEXT");
            recordFulfill(connection, "API", "ALICE", parameterThreadId);

            System.out.println();
            System.out.println("Step 6: verify converged ledger invariants");
            runVerify(connection);

            System.out.println();
            System.out.println("Governing objects");
            printQuery(connection,
                    "select object_kind, object_key, object_name, governing_thread_id " +
                    "from api_ledger.v_registry_object order by object_kind, object_key");

            System.out.println();
            System.out.println("Governing relations");
            printQuery(connection,
                    "select relation_id, source_object_id, relation_type, target_object_id, thread_id " +
                    "from api_ledger.v_registry_relation order by relation_id");

            System.out.println();
            System.out.println("Recorded attributes");
            printQuery(connection,
                    "select oa.object_attr_id, o.object_kind, o.object_key, oa.attr_name, oa.attr_value, oa.value_type, oa.thread_id " +
                    "from api_ledger.api_object_attr oa " +
                    "join api_ledger.api_object o on o.object_id = oa.object_id " +
                    "order by oa.object_attr_id");

            System.out.println();
            System.out.println("Demo conclusion");
            System.out.println("A thread can declare relations outward, but object state stays object-local.");
            System.out.println("Subordinate objects get their own threads when they need governed state.");
        }
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback;
    }

    private static void setSearchPath(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("set search_path = api_ledger");
        }
    }

    private static void resetLedger(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(RESET_SQL);
        }
        System.out.println("ledger reset");
    }

    private static void seedProtocolObjects(Connection connection) throws SQLException {
        executeUpdate(connection,
                "insert into api_ledger.api_stream(stream_code, stream_title) values ('API', 'API Stream')");
        executeUpdate(connection,
                "insert into api_ledger.api_participant(participant_code, display_name) values ('ALICE', 'Alice')");
        executeUpdate(connection,
                "insert into api_ledger.api_object(stream_id, object_kind, object_key, object_name) " +
                "select stream_id, 'ENDPOINT', 'EP_HELLO', 'Hello Endpoint' " +
                "from api_ledger.api_stream where stream_code = 'API'");
        executeUpdate(connection,
                "insert into api_ledger.api_object(stream_id, object_kind, object_key, object_name) " +
                "select stream_id, 'PARAMETER', 'P_NAME', 'Name Parameter' " +
                "from api_ledger.api_stream where stream_code = 'API'");
        System.out.println("seed installed");
    }

    private static void executeUpdate(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }

    private static long recordCommit(Connection connection, String streamCode, String participantCode,
                                     String objectKind, String objectKey) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select act_id, act_seq, thread_id from api_ledger.record_commit(?, ?, ?, ?, null)")) {
            ps.setString(1, streamCode);
            ps.setString(2, participantCode);
            ps.setString(3, objectKind);
            ps.setString(4, objectKey);

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long actId = rs.getLong("act_id");
                long actSeq = rs.getLong("act_seq");
                long threadId = rs.getLong("thread_id");
                System.out.printf("commit -> act_id=%d act_seq=%d thread_id=%d%n", actId, actSeq, threadId);
                return threadId;
            }
        }
    }

    private static void recordRelate(Connection connection, String streamCode, String participantCode,
                                     long threadId, String sourceKind, String sourceKey, String relationType,
                                     String targetKind, String targetKey, Integer ordinalNo) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select relation_id, created_by_act_id, act_seq " +
                "from api_ledger.record_relate(?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
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
                rs.next();
                System.out.printf("relate -> relation_id=%d act_id=%d act_seq=%d%n",
                        rs.getLong("relation_id"),
                        rs.getLong("created_by_act_id"),
                        rs.getLong("act_seq"));
            }
        }
    }

    private static void recordAttribute(Connection connection, String streamCode, String participantCode,
                                        long threadId, String objectKind, String objectKey,
                                        String attrName, String attrValue, String valueType) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select object_attr_id, created_by_act_id, act_seq " +
                "from api_ledger.record_attribute(?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, streamCode);
            ps.setString(2, participantCode);
            ps.setLong(3, threadId);
            ps.setString(4, objectKind);
            ps.setString(5, objectKey);
            ps.setString(6, attrName);
            ps.setString(7, attrValue);
            if (valueType == null || valueType.isBlank()) {
                ps.setNull(8, java.sql.Types.VARCHAR);
            } else {
                ps.setString(8, valueType);
            }

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                System.out.printf("attribute -> object_attr_id=%d act_id=%d act_seq=%d%n",
                        rs.getLong("object_attr_id"),
                        rs.getLong("created_by_act_id"),
                        rs.getLong("act_seq"));
            }
        }
    }

    private static void recordFulfill(Connection connection, String streamCode, String participantCode,
                                      long threadId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select act_id, act_seq from api_ledger.record_fulfill(?, ?, ?)")) {
            ps.setString(1, streamCode);
            ps.setString(2, participantCode);
            ps.setLong(3, threadId);

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                System.out.printf("fulfill -> act_id=%d act_seq=%d%n",
                        rs.getLong("act_id"), rs.getLong("act_seq"));
            }
        }
    }

    private static void runVerify(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create or replace function api_ledger.assert_count(p_sql text, p_expected bigint, p_msg text) returns void language plpgsql as $$ declare v bigint; begin execute format('select count(*) from (%s) q', p_sql) into v; if v <> p_expected then raise exception 'ASSERT FAILED: %, expected %, got %', p_msg, p_expected, v; end if; end; $$;");
            statement.execute("create or replace function api_ledger.assert_zero(p_sql text, p_msg text) returns void language plpgsql as $$ begin perform api_ledger.assert_count(p_sql, 0, p_msg); end; $$;");
            statement.execute("do $$ begin perform api_ledger.assert_zero($query$ select * from api_ledger.v_ambiguous_object $query$, 'no object may have multiple governing threads'); perform api_ledger.assert_zero($query$ select a.stream_id, a.object_id from ( select stream_id, object_id from api_ledger.v_accepted_thread group by stream_id, object_id ) a left join api_ledger.v_governing_thread g on g.stream_id = a.stream_id and g.object_id = a.object_id where g.thread_id is null $query$, 'accepted objects must converge to a governing thread'); perform api_ledger.assert_zero($query$ select open_t.stream_id, open_t.object_id, open_t.thread_id from api_ledger.v_thread_status open_t join api_ledger.v_governing_thread g on g.stream_id = open_t.stream_id and g.object_id = open_t.object_id where open_t.thread_status = 'OPEN' $query$, 'no open thread may coexist with a governing thread for the same object'); perform api_ledger.assert_zero($query$ select r.relation_id from api_ledger.v_registry_relation r left join api_ledger.v_governing_thread g on g.thread_id = r.thread_id where g.thread_id is null $query$, 'governed relations must be attached to governing threads'); perform api_ledger.assert_zero($query$ select r.relation_id from api_ledger.api_relation r join api_ledger.v_thread_status ts on ts.thread_id = r.thread_id where ts.thread_status = 'RESTARTED' $query$, 'no relation may remain on a restarted thread'); perform api_ledger.assert_zero($query$ select object_attr_id from api_ledger.v_attribute_status where attribute_semantics in ('UNRESOLVED', 'UNKNOWN') $query$, 'attributes must resolve to draft, void, historical, or authoritative semantics'); perform api_ledger.assert_zero($query$ select object_attr_id from api_ledger.v_registry_attribute where attribute_semantics <> 'AUTHORITATIVE' $query$, 'authoritative attribute registry may only contain authoritative attributes'); perform api_ledger.assert_zero($query$ with seqs as ( select stream_id, act_seq, row_number() over (partition by stream_id order by act_seq) as expected_seq from api_ledger.api_act ) select * from seqs where act_seq <> expected_seq $query$, 'act_seq must be dense per stream'); end $$;");
        }
        System.out.println("verify -> PASS");
    }

    private static void expectSemanticFailure(Connection connection, String label, SqlRunnable runnable) throws SQLException {
        try {
            runnable.run();
            throw new IllegalStateException("Expected failure did not occur for: " + label);
        } catch (SQLException e) {
            System.out.println(label + " -> expected failure");
            System.out.println("  " + summarizeSqlException(e));
        }
    }

    private static void printQuery(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int columns = meta.getColumnCount();

            for (int i = 1; i <= columns; i++) {
                if (i > 1) {
                    System.out.print(" | ");
                }
                System.out.print(meta.getColumnLabel(i));
            }
            System.out.println();

            boolean hasRows = false;
            while (rs.next()) {
                hasRows = true;
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        System.out.print(" | ");
                    }
                    Object value = rs.getObject(i);
                    System.out.print(value == null ? "null" : value.toString());
                }
                System.out.println();
            }

            if (!hasRows) {
                System.out.println("(no rows)");
            }
        }
    }

    private static String summarizeSqlException(SQLException e) {
        StringBuilder sb = new StringBuilder();
        SQLException current = e;
        boolean first = true;
        while (current != null) {
            if (!first) {
                sb.append(" | ");
            }
            sb.append(current.getMessage());
            current = current.getNextException();
            first = false;
        }
        return sb.toString();
    }

    @FunctionalInterface
    private interface SqlRunnable {
        void run() throws SQLException;
    }
}
