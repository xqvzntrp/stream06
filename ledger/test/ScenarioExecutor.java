package ledger.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

public final class ScenarioExecutor {

    private final SqlScriptRunner scriptRunner = new SqlScriptRunner();

    public void runSqlFile(Connection cx, Path file) throws Exception {
        String sql = Files.readString(file);
        scriptRunner.run(cx, sql);
    }

    public void runScenario(Scenario scenario, Connection cx) throws Exception {
        scenario.run(cx);
    }
}
