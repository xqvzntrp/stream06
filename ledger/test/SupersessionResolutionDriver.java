package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class SupersessionResolutionDriver extends CompetingAcceptanceDriver {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public SupersessionResolutionDriver(String jdbcUrl, String user, String password) {
        super(jdbcUrl, user, password);
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password == null ? "" : password;
    }

    @Override
    public void run() throws Exception {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            seed(cx);

            long thread1 = recordCommit(cx);
            long thread2 = recordCommit(cx);
            recordFulfill(cx, thread1);
            recordFulfill(cx, thread2);
            recordSupersede(cx, thread2, thread1);
        }
    }

    public long ambiguousObjectCount() throws SQLException {
        return queryCount("select count(*) from api_ledger.v_ambiguous_object");
    }

    public long governingThreadCount() throws SQLException {
        return queryCount("select count(*) from api_ledger.v_governing_thread");
    }

    protected long queryCount(String sql) throws SQLException {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            try (PreparedStatement ps = cx.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }
}
