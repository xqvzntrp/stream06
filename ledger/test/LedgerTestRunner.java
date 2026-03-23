package ledger.test;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;

public final class LedgerTestRunner {

    private static final String ENV_ADMIN_URL = "LEDGER_ADMIN_URL";
    private static final String ENV_DB_USER = "LEDGER_DB_USER";
    private static final String ENV_DB_PASSWORD = "LEDGER_DB_PASSWORD";

    private static final Path TABLES_SQL = Path.of("ledger_tables.sql");
    private static final Path VIEWS_SQL = Path.of("ledger_views.sql");
    private static final Path PROCEDURES_SQL = Path.of("ledger_procedures.sql");

    private static final int CONCURRENCY_WORKERS = 16;
    private static final int COMMITS_PER_WORKER = 200;

    private LedgerTestRunner() {
    }

    public static void main(String[] args) throws Exception {
        String adminUrl = firstNonBlank(
                System.getenv(ENV_ADMIN_URL),
                "jdbc:postgresql://localhost:5432/postgres"
        );
        String user = firstNonBlank(
                System.getenv(ENV_DB_USER),
                firstNonBlank(System.getenv("PGUSER"), System.getProperty("user.name", "postgres"))
        );
        String password = firstNonBlank(
                System.getenv(ENV_DB_PASSWORD),
                System.getenv("PGPASSWORD")
        );

        DatabaseLifecycle lifecycle = new DatabaseLifecycle(adminUrl, user, password);
        ScenarioExecutor scenarioExecutor = new ScenarioExecutor();
        VerificationExecutor verifier = new VerificationExecutor();
        List<Scenario> scenarios = List.of(new ScenarioBasicAccept());

        for (Scenario scenario : scenarios) {
            runScenarioSuite(lifecycle, scenarioExecutor, verifier, scenario);
        }

        runCommitStressSuite(lifecycle, scenarioExecutor, verifier, user, password);
        runMixedLifecycleSuite(lifecycle, scenarioExecutor, verifier, user, password);
        runCompetingAcceptanceSuite(lifecycle, scenarioExecutor, verifier, user, password);

        System.out.println();
        System.out.println("ALL SCENARIOS PASSED");
    }

    private static void runScenarioSuite(DatabaseLifecycle lifecycle,
                                         ScenarioExecutor scenarioExecutor,
                                         VerificationExecutor verifier,
                                         Scenario scenario) throws Exception {
        System.out.println();
        System.out.println("=== RUNNING SCENARIO: " + scenario.name());

        DatabaseLifecycle.TestDatabase db = lifecycle.createFreshDatabase();
        Connection cx = db.connection;

        try {
            System.out.println("database: " + db.name);
            installProtocol(cx, scenarioExecutor);
            scenarioExecutor.runScenario(scenario, cx);
            verifyOutcome(verifier, cx, SuiteOutcome.EXPECT_PASS);
            System.out.println("PASS: " + scenario.name());
        } finally {
            try {
                cx.close();
            } finally {
                lifecycle.dropDatabase(db.name);
            }
        }
    }

    private static void runCommitStressSuite(DatabaseLifecycle lifecycle,
                                             ScenarioExecutor scenarioExecutor,
                                             VerificationExecutor verifier,
                                             String user,
                                             String password) throws Exception {
        System.out.println();
        System.out.println("=== RUNNING CONCURRENCY: commit-stress");

        DatabaseLifecycle.TestDatabase db = lifecycle.createFreshDatabase();
        Connection cx = db.connection;

        try {
            System.out.println("database: " + db.name);
            installProtocol(cx, scenarioExecutor);

            ConcurrencyDriver driver = new ConcurrencyDriver(
                    db.url,
                    user,
                    password,
                    verifier,
                    CONCURRENCY_WORKERS,
                    COMMITS_PER_WORKER
            );
            driver.run();
            System.out.println("PASS: commit-stress");
        } finally {
            try {
                cx.close();
            } finally {
                lifecycle.dropDatabase(db.name);
            }
        }
    }

    private static void runMixedLifecycleSuite(DatabaseLifecycle lifecycle,
                                               ScenarioExecutor scenarioExecutor,
                                               VerificationExecutor verifier,
                                               String user,
                                               String password) throws Exception {
        System.out.println();
        System.out.println("=== RUNNING CONCURRENCY: mixed-lifecycle");

        DatabaseLifecycle.TestDatabase db = lifecycle.createFreshDatabase();
        Connection cx = db.connection;

        try {
            System.out.println("database: " + db.name);
            installProtocol(cx, scenarioExecutor);

            MixedLifecycleDriver driver = new MixedLifecycleDriver(
                    db.url,
                    user,
                    password,
                    verifier,
                    CONCURRENCY_WORKERS,
                    COMMITS_PER_WORKER
            );
            driver.run();
            System.out.println("PASS: mixed-lifecycle");
        } finally {
            try {
                cx.close();
            } finally {
                lifecycle.dropDatabase(db.name);
            }
        }
    }

    private static void runCompetingAcceptanceSuite(DatabaseLifecycle lifecycle,
                                                    ScenarioExecutor scenarioExecutor,
                                                    VerificationExecutor verifier,
                                                    String user,
                                                    String password) throws Exception {
        System.out.println();
        System.out.println("=== RUNNING COLLISION: competing-acceptance");

        DatabaseLifecycle.TestDatabase db = lifecycle.createFreshDatabase();
        Connection cx = db.connection;

        try {
            System.out.println("database: " + db.name);
            installProtocol(cx, scenarioExecutor);

            CompetingAcceptanceDriver driver = new CompetingAcceptanceDriver(db.url, user, password);
            driver.run();
            verifyOutcome(verifier, cx, SuiteOutcome.EXPECT_FAIL);

            List<String> ambiguousRows = driver.ambiguousObjects();
            if (ambiguousRows.isEmpty()) {
                throw new IllegalStateException("expected ambiguity rows but found none");
            }
            System.out.println("ambiguous objects:");
            for (String row : ambiguousRows) {
                System.out.println("  " + row);
            }

            System.out.println("PASS: competing-acceptance (expected verification failure)");
        } finally {
            try {
                cx.close();
            } finally {
                lifecycle.dropDatabase(db.name);
            }
        }
    }

    private static void installProtocol(Connection cx, ScenarioExecutor scenarioExecutor) throws Exception {
        scenarioExecutor.runSqlFile(cx, TABLES_SQL);
        scenarioExecutor.runSqlFile(cx, VIEWS_SQL);
        scenarioExecutor.runSqlFile(cx, PROCEDURES_SQL);
    }

    private static void verifyOutcome(VerificationExecutor verifier,
                                      Connection cx,
                                      SuiteOutcome expectedOutcome) throws Exception {
        try {
            verifier.verify(cx);
            if (expectedOutcome == SuiteOutcome.EXPECT_FAIL) {
                throw new IllegalStateException("verification passed but failure was expected");
            }
        } catch (Exception e) {
            if (expectedOutcome == SuiteOutcome.EXPECT_FAIL) {
                System.out.println("expected verification failure: " + e.getMessage());
                return;
            }
            throw e;
        }
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred;
        }
        return fallback == null ? "" : fallback;
    }
}
