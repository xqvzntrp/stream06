package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class MixedLifecycleDriver {

    private static final String STREAM_CODE = "API";
    private static final String PARTICIPANT_CODE = "ALICE";
    private static final String OBJECT_KIND = "ENDPOINT";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final VerificationExecutor verifier;
    private final int workers;
    private final int objectsPerWorker;

    public MixedLifecycleDriver(String jdbcUrl, String user, String password,
                                VerificationExecutor verifier, int workers, int objectsPerWorker) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password == null ? "" : password;
        this.verifier = verifier;
        this.workers = workers;
        this.objectsPerWorker = objectsPerWorker;
    }

    public void run() throws Exception {
        int totalObjects = workers * objectsPerWorker;

        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            seed(cx, totalObjects);
        }

        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch startGate = new CountDownLatch(1);

        for (int workerIndex = 0; workerIndex < workers; workerIndex++) {
            final int index = workerIndex;
            pool.submit(() -> runWorker(index, startGate, firstFailure));
        }

        startGate.countDown();
        pool.shutdown();

        boolean terminated = pool.awaitTermination(10, TimeUnit.MINUTES);
        if (!terminated) {
            pool.shutdownNow();
            throw new IllegalStateException("mixed lifecycle test timed out");
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw new IllegalStateException("mixed lifecycle worker failed", failure);
        }

        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            verifier.verify(cx);
            assertLifecycleCoverage(cx, totalObjects);
        }
    }

    private void seed(Connection cx, int totalObjects) throws SQLException {
        try (Statement st = cx.createStatement()) {
            st.execute("insert into api_ledger.api_stream(stream_code, stream_title) values ('API', 'API Stream')");
            st.execute("insert into api_ledger.api_participant(participant_code, display_name) values ('ALICE', 'Alice')");
        }

        try (PreparedStatement ps = cx.prepareStatement(
                "insert into api_ledger.api_object(stream_id, object_kind, object_key, object_name) " +
                "select stream_id, ?, ?, ? from api_ledger.api_stream where stream_code = ?")) {
            for (int i = 1; i <= totalObjects; i++) {
                String objectKey = objectKey(i);
                ps.setString(1, OBJECT_KIND);
                ps.setString(2, objectKey);
                ps.setString(3, "Endpoint " + objectKey);
                ps.setString(4, STREAM_CODE);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void runWorker(int workerIndex, CountDownLatch startGate, AtomicReference<Throwable> firstFailure) {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            startGate.await();

            int start = workerIndex * objectsPerWorker + 1;
            int end = start + objectsPerWorker;

            for (int i = start; i < end; i++) {
                if (firstFailure.get() != null) {
                    return;
                }

                long threadId = recordCommit(cx, objectKey(i));
                if (shouldFulfill(i)) {
                    recordFulfill(cx, threadId);
                } else {
                    recordRestart(cx, threadId);
                }
            }
        } catch (Throwable t) {
            if (firstFailure.compareAndSet(null, t)) {
                System.err.println("mixed worker " + workerIndex + " failed: " + t.getMessage());
                t.printStackTrace(System.err);
            }
        }
    }

    private long recordCommit(Connection cx, String objectKey) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select act_id, act_seq, thread_id from api_ledger.record_commit(?, ?, ?, ?, null)")) {
            ps.setString(1, STREAM_CODE);
            ps.setString(2, PARTICIPANT_CODE);
            ps.setString(3, OBJECT_KIND);
            ps.setString(4, objectKey);
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

    private void recordRestart(Connection cx, long threadId) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select act_id, act_seq from api_ledger.record_restart(?, ?, ?)")) {
            ps.setString(1, STREAM_CODE);
            ps.setString(2, PARTICIPANT_CODE);
            ps.setLong(3, threadId);
            ps.executeQuery().close();
        }
    }

    private void assertLifecycleCoverage(Connection cx, int totalObjects) throws SQLException {
        long expectedActs = totalObjects * 2L;
        long expectedFulfill = expectedFulfillCount(totalObjects);
        long expectedRestart = totalObjects - expectedFulfill;

        try (PreparedStatement ps = cx.prepareStatement(
                "select count(*) as act_count, min(act_seq) as min_seq, max(act_seq) as max_seq from api_ledger.api_act");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            long actCount = rs.getLong("act_count");
            long minSeq = rs.getLong("min_seq");
            long maxSeq = rs.getLong("max_seq");

            if (actCount != expectedActs) {
                throw new IllegalStateException("expected " + expectedActs + " acts but found " + actCount);
            }
            if (expectedActs > 0 && minSeq != 1) {
                throw new IllegalStateException("expected min act_seq=1 but found " + minSeq);
            }
            if (expectedActs > 0 && maxSeq != expectedActs) {
                throw new IllegalStateException("expected max act_seq=" + expectedActs + " but found " + maxSeq);
            }
        }

        assertCount(cx,
                "select count(*) from api_ledger.v_thread_status where thread_status = 'ACCEPTED'",
                expectedFulfill,
                "accepted thread count");
        assertCount(cx,
                "select count(*) from api_ledger.v_thread_status where thread_status = 'RESTARTED'",
                expectedRestart,
                "restarted thread count");
        assertCount(cx,
                "select count(*) from api_ledger.v_thread_status where thread_status = 'OPEN'",
                0,
                "open thread count");
        assertCount(cx,
                "select count(*) from api_ledger.api_thread where closed_by_act_id is null or closed_ts is null or closure_type is null",
                0,
                "table-level incomplete closure rows");
    }

    private void assertCount(Connection cx, String sql, long expected, String label) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            long actual = rs.getLong(1);
            if (actual != expected) {
                throw new IllegalStateException(label + " expected=" + expected + " actual=" + actual);
            }
        }
    }

    private void initializeConnection(Connection cx) throws SQLException {
        cx.setAutoCommit(true);
        try (Statement st = cx.createStatement()) {
            st.execute("set search_path = api_ledger");
        }
    }

    private static boolean shouldFulfill(int objectIndex) {
        return objectIndex % 2 == 0;
    }

    private static long expectedFulfillCount(int totalObjects) {
        return totalObjects / 2L;
    }

    private static String objectKey(int index) {
        return String.format("EP_%06d", index);
    }
}
