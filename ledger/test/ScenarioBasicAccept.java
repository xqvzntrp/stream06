package ledger.test;

import java.nio.file.Path;
import java.sql.Connection;

public final class ScenarioBasicAccept implements Scenario {

    private final ScenarioExecutor executor = new ScenarioExecutor();

    @Override
    public String name() {
        return "basic-accept";
    }

    @Override
    public void run(Connection cx) throws Exception {
        executor.runSqlFile(cx, Path.of("ledger_scenario01.sql"));
    }
}
