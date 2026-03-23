import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * SupersessionResolutionDemo
 *
 * Standalone database-backed demo of ambiguity and supersession:
 * accepted proposals can compete, ambiguity is explicit, and supersession
 * restores a single governing truth.
 *
 * Usage:
 *   javac SupersessionResolutionDemo.java
 *   LEDGER_DB_PASSWORD=alex java -cp /home/alex/Documents/stream06:/home/alex/Documents/stream06/postgresql-42.7.10.jar SupersessionResolutionDemo
 */
public final class SupersessionResolutionDemo {

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

    private SupersessionResolutionDemo() {
    }

    public static void main(String[] args) throws Exception {
        if (DEFAULT_DB_PASSWORD == null || DEFAULT_DB_PASSWORD.isBlank()) {
            throw new IllegalStateException("Set LEDGER_DB_PASSWORD or PGPASSWORD before running this demo.");
        }

        try (Connection connection = DriverManager.getConnection(DEFAULT_JDBC_URL, DEFAULT_DB_USER, DEFAULT_DB_PASSWORD)) {
            connection.setAutoCommit(true);
            setSearchPath(connection);

            System.out.println("========================================");
            System.out.println("Supersession Resolution Demo");
            System.out.println("========================================");
            System.out.println("JDBC URL: " + DEFAULT_JDBC_URL);
            System.out.println("User:     " + DEFAULT_DB_USER);

            resetLedger(connection);
            seedProtocolObjects(connection);

            System.out.println();
            System.out.println("Step 1: accept an initial endpoint definition");
            long thread1 = recordCommit(connection, "API", "ALICE", "ENDPOINT", "EP_HELLO");
            recordAttribute(connection, "API", "ALICE", thread1,
                    "ENDPOINT", "EP_HELLO", "DESCRIPTION", "Version One", "TEXT");
            recordFulfill(connection, "API", "ALICE", thread1);
            printState(connection, "After initial acceptance");
            runVerifyExpectPass(connection);

            System.out.println();
            System.out.println("Step 2: accept a competing redefinition of the same endpoint");
            long thread2 = recordCommit(connection, "API", "ALICE", "ENDPOINT", "EP_HELLO");
            recordAttribute(connection, "API", "ALICE", thread2,
                    "ENDPOINT", "EP_HELLO", "DESCRIPTION", "Version Two", "TEXT");
            recordFulfill(connection, "API", "ALICE", thread2);
            printState(connection, "Ambiguity after competing acceptance");
            runVerifyExpectFailure(connection);

            System.out.println();
            System.out.println("Step 3: supersede the older accepted thread with the newer one");
            recordSupersede(connection, "API", "ALICE", thread2, thread1);
            printState(connection, "After supersession resolution");
            runVerifyExpectPass(connection);

            System.out.println();
            System.out.println("Recorded endpoint attributes");
            printQuery(connection,
                    "select oa.object_attr_id, oa.attr_name, oa.attr_value, oa.thread_id " +
                    "from api_ledger.api_object_attr oa " +
                    "join api_ledger.api_object o on o.object_id = oa.object_id " +
                    "where o.object_kind = 'ENDPOINT' and o.object_key = 'EP_HELLO' " +
                    "order by oa.object_attr_id");

            System.out.println();
            System.out.println("Demo conclusion");
            System.out.println("Supersession restores a single governing thread.");
            System.out.println("Historical accepted attributes remain in the ledger, while authoritative state moves to the governing thread.");
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

    private static void recordAttribute(Connection connection, String streamCode, String participantCode,
                                        long threadId, String objectKind, String objectKey,
                                        String attrName, String attrValue, String valueType) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select object_attr_id, created_by_act_id, act_seq from api_ledger.record_attribute(?, ?, ?, ?, ?, ?, ?, ?)")) {
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

    private static void recordSupersede(Connection connection, String streamCode, String participantCode,
                                        long supersedingThreadId, long supersededThreadId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select act_id, act_seq, thread_supersession_id from api_ledger.record_supersede(?, ?, ?, ?)")) {
            ps.setString(1, streamCode);
            ps.setString(2, participantCode);
            ps.setLong(3, supersedingThreadId);
            ps.setLong(4, supersededThreadId);

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                System.out.printf("supersede -> act_id=%d act_seq=%d thread_supersession_id=%d%n",
                        rs.getLong("act_id"),
                        rs.getLong("act_seq"),
                        rs.getLong("thread_supersession_id"));
            }
        }
    }

    private static void runVerifyExpectPass(Connection connection) throws SQLException {
        runVerify(connection);
        System.out.println("verify -> PASS");
    }

    private static void runVerifyExpectFailure(Connection connection) throws SQLException {
        try {
            runVerify(connection);
            throw new IllegalStateException("Expected verification failure during ambiguity, but verification passed.");
        } catch (SQLException e) {
            System.out.println("verify -> expected failure");
            System.out.println("  " + summarizeSqlException(e));
        }
    }


    private static void runVerify(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create or replace function api_ledger.assert_count(p_sql text, p_expected bigint, p_msg text) returns void language plpgsql as $$ declare v bigint; begin execute format('select count(*) from (%s) q', p_sql) into v; if v <> p_expected then raise exception 'ASSERT FAILED: %, expected %, got %', p_msg, p_expected, v; end if; end; $$;");
            statement.execute("create or replace function api_ledger.assert_zero(p_sql text, p_msg text) returns void language plpgsql as $$ begin perform api_ledger.assert_count(p_sql, 0, p_msg); end; $$;");
            statement.execute("do $$ begin perform api_ledger.assert_zero($query$ select * from api_ledger.v_ambiguous_object $query$, 'no object may have multiple governing threads'); perform api_ledger.assert_zero($query$ select a.stream_id, a.object_id from ( select stream_id, object_id from api_ledger.v_accepted_thread group by stream_id, object_id ) a left join api_ledger.v_governing_thread g on g.stream_id = a.stream_id and g.object_id = a.object_id where g.thread_id is null $query$, 'accepted objects must converge to a governing thread'); perform api_ledger.assert_zero($query$ select open_t.stream_id, open_t.object_id, open_t.thread_id from api_ledger.v_thread_status open_t join api_ledger.v_governing_thread g on g.stream_id = open_t.stream_id and g.object_id = open_t.object_id where open_t.thread_status = 'OPEN' $query$, 'no open thread may coexist with a governing thread for the same object'); perform api_ledger.assert_zero($query$ select r.relation_id from api_ledger.v_registry_relation r left join api_ledger.v_governing_thread g on g.thread_id = r.thread_id where g.thread_id is null $query$, 'governed relations must be attached to governing threads'); perform api_ledger.assert_zero($query$ select r.relation_id from api_ledger.api_relation r join api_ledger.v_thread_status ts on ts.thread_id = r.thread_id where ts.thread_status = 'RESTARTED' $query$, 'no relation may remain on a restarted thread'); perform api_ledger.assert_zero($query$ select object_attr_id from api_ledger.v_attribute_status where attribute_semantics in ('UNRESOLVED', 'UNKNOWN') $query$, 'attributes must resolve to draft, void, historical, or authoritative semantics'); perform api_ledger.assert_zero($query$ select object_attr_id from api_ledger.v_registry_attribute where attribute_semantics <> 'AUTHORITATIVE' $query$, 'authoritative attribute registry may only contain authoritative attributes'); perform api_ledger.assert_zero($query$ with seqs as ( select stream_id, act_seq, row_number() over (partition by stream_id order by act_seq) as expected_seq from api_ledger.api_act ) select * from seqs where act_seq <> expected_seq $query$, 'act_seq must be dense per stream'); end $$;");
        }
    }

    private static void printState(Connection connection, String label) throws SQLException {
        System.out.println();
        System.out.println(label);
        System.out.println("Thread status");
        printQuery(connection,
                "select thread_id, object_id, thread_status, closure_type from api_ledger.v_thread_status order by thread_id");
        System.out.println();
        System.out.println("Ambiguous objects");
        printQuery(connection,
                "select stream_id, object_id, governing_thread_count from api_ledger.v_ambiguous_object order by object_id");
        System.out.println();
        System.out.println("Governing objects");
        printQuery(connection,
                "select object_kind, object_key, object_name, governing_thread_id from api_ledger.v_registry_object order by object_kind, object_key");
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
}
