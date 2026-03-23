package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;

public final class RestartVsFulfillDriver extends CompetingAcceptanceDriver {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public RestartVsFulfillDriver(String jdbcUrl, String user, String password) {
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
            recordRestart(cx, thread2);
        }
    }

    public long ambiguousObjectCount() throws Exception {
        return queryCount("select count(*) from api_ledger.v_ambiguous_object");
    }

    public long governingThreadCount() throws Exception {
        return queryCount("select count(*) from api_ledger.v_governing_thread");
    }
}
