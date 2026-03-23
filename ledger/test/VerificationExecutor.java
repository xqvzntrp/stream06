package ledger.test;

import java.nio.file.Path;
import java.sql.Connection;

public final class VerificationExecutor {

    private final ScenarioExecutor executor = new ScenarioExecutor();

    public void verify(Connection cx) throws Exception {
        executor.runSqlFile(cx, Path.of("ledger_verify.sql"));
    }
}
