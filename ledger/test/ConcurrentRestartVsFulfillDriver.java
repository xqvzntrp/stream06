package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ConcurrentRestartVsFulfillDriver extends CompetingAcceptanceDriver {

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public ConcurrentRestartVsFulfillDriver(String jdbcUrl, String user, String password) {
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
        }

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch startGate = new CountDownLatch(1);

        pool.submit(() -> runWorker(startGate, firstFailure, true));
        pool.submit(() -> runWorker(startGate, firstFailure, false));

        startGate.countDown();
        pool.shutdown();

        boolean terminated = pool.awaitTermination(10, TimeUnit.MINUTES);
        if (!terminated) {
            pool.shutdownNow();
            throw new IllegalStateException("concurrent restart-vs-fulfill test timed out");
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw new IllegalStateException("concurrent restart-vs-fulfill worker failed", failure);
        }
    }

    private void runWorker(CountDownLatch startGate,
                           AtomicReference<Throwable> firstFailure,
                           boolean fulfill) {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            startGate.await();

            if (firstFailure.get() != null) {
                return;
            }

            long threadId = recordCommit(cx);
            if (firstFailure.get() != null) {
                return;
            }

            if (fulfill) {
                recordFulfill(cx, threadId);
            } else {
                recordRestart(cx, threadId);
            }
        } catch (Throwable t) {
            firstFailure.compareAndSet(null, t);
        }
    }

    public long ambiguousObjectCount() throws Exception {
        return queryCount("select count(*) from api_ledger.v_ambiguous_object");
    }

    public long governingThreadCount() throws Exception {
        return queryCount("select count(*) from api_ledger.v_governing_thread");
    }
}
