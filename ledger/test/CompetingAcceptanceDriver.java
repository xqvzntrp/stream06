package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class CompetingAcceptanceDriver {

    private static final String STREAM_CODE = "API";
    private static final String PARTICIPANT_CODE = "ALICE";
    private static final String OBJECT_KIND = "ENDPOINT";
    private static final String OBJECT_KEY = "EP_COLLISION";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public CompetingAcceptanceDriver(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password == null ? "" : password;
    }

    public void run() throws Exception {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            seed(cx);

            long threadA = recordCommit(cx);
            long threadB = recordCommit(cx);
            recordFulfill(cx, threadA);
            recordFulfill(cx, threadB);
        }
    }

    public List<String> ambiguousObjects() throws SQLException {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            return queryAmbiguousObjects(cx);
        }
    }

    private void seed(Connection cx) throws SQLException {
        try (Statement st = cx.createStatement()) {
            st.execute("insert into api_ledger.api_stream(stream_code, stream_title) values ('API', 'API Stream')");
            st.execute("insert into api_ledger.api_participant(participant_code, display_name) values ('ALICE', 'Alice')");
            st.execute(
                    "insert into api_ledger.api_object(stream_id, object_kind, object_key, object_name) " +
                    "select stream_id, 'ENDPOINT', 'EP_COLLISION', 'Collision Endpoint' " +
                    "from api_ledger.api_stream where stream_code = 'API'"
            );
        }
    }

    private long recordCommit(Connection cx) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select act_id, act_seq, thread_id from api_ledger.record_commit(?, ?, ?, ?, null)")) {
            ps.setString(1, STREAM_CODE);
            ps.setString(2, PARTICIPANT_CODE);
            ps.setString(3, OBJECT_KIND);
            ps.setString(4, OBJECT_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong("thread_id");
            }
        }
    }

    private void recordFulfill(Connection cx, long threadId) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select act_id, act_seq from api_ledger.record_fulfill(?, ?, ?)")) {
            ps.setString(1, STREAM_CODE);
            ps.setString(2, PARTICIPANT_CODE);
            ps.setLong(3, threadId);
            ps.executeQuery().close();
        }
    }

    private List<String> queryAmbiguousObjects(Connection cx) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select stream_id, object_id, governing_thread_count from api_ledger.v_ambiguous_object order by stream_id, object_id");
             ResultSet rs = ps.executeQuery()) {
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(
                        "stream_id=" + rs.getLong("stream_id") +
                        ", object_id=" + rs.getLong("object_id") +
                        ", governing_thread_count=" + rs.getLong("governing_thread_count")
                );
            }
            return rows;
        }
    }

    private void initializeConnection(Connection cx) throws SQLException {
        cx.setAutoCommit(true);
        try (Statement st = cx.createStatement()) {
            st.execute("set search_path = api_ledger");
        }
    }
}
