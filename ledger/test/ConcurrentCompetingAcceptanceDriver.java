package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ConcurrentCompetingAcceptanceDriver extends CompetingAcceptanceDriver {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final int workers;

    public ConcurrentCompetingAcceptanceDriver(String jdbcUrl, String user, String password, int workers) {
        super(jdbcUrl, user, password);
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password == null ? "" : password;
        this.workers = workers;
    }

    @Override
    public void run() throws Exception {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            seed(cx);
        }

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch startGate = new CountDownLatch(1);

        for (int workerIndex = 0; workerIndex < workers; workerIndex++) {
            pool.submit(() -> runWorker(startGate, firstFailure));
        }

        startGate.countDown();
        pool.shutdown();

        boolean terminated = pool.awaitTermination(10, TimeUnit.MINUTES);
        if (!terminated) {
            pool.shutdownNow();
            throw new IllegalStateException("concurrent competing-acceptance test timed out");
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw new IllegalStateException("concurrent competing-acceptance worker failed", failure);
        }
    }

    private void runWorker(CountDownLatch startGate, AtomicReference<Throwable> firstFailure) {
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
            recordFulfill(cx, threadId);
        } catch (Throwable t) {
            firstFailure.compareAndSet(null, t);
        }
    }
}
