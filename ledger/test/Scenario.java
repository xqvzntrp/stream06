package ledger.test;

import java.sql.Connection;

public interface Scenario {
    String name();
    void run(Connection cx) throws Exception;
}
