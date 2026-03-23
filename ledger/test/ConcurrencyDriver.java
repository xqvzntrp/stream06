package ledger.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class ConcurrencyDriver {

    private static final String STREAM_CODE = "API";
    private static final String PARTICIPANT_CODE = "ALICE";
    private static final String OBJECT_KIND = "ENDPOINT";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final VerificationExecutor verifier;
    private final int workers;
    private final int commitsPerWorker;

    public ConcurrencyDriver(String jdbcUrl, String user, String password,
                             VerificationExecutor verifier, int workers, int commitsPerWorker) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password == null ? "" : password;
        this.verifier = verifier;
        this.workers = workers;
        this.commitsPerWorker = commitsPerWorker;
    }

    public void run() throws Exception {
        int totalWrites = workers * commitsPerWorker;

        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            seed(cx, totalWrites);
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
            throw new IllegalStateException("concurrency test timed out");
        }

        Throwable failure = firstFailure.get();
        if (failure != null) {
            dumpActSeqSample();
            throw new IllegalStateException("concurrency worker failed", failure);
        }

        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password)) {
            initializeConnection(cx);
            verifier.verify(cx);
            assertActCoverage(cx, totalWrites);
        }
    }

    private void seed(Connection cx, int totalWrites) throws SQLException {
        try (Statement st = cx.createStatement()) {
            st.execute("insert into api_ledger.api_stream(stream_code, stream_title) values ('API', 'API Stream')");
            st.execute("insert into api_ledger.api_participant(participant_code, display_name) values ('ALICE', 'Alice')");
        }

        try (PreparedStatement ps = cx.prepareStatement(
                "insert into api_ledger.api_object(stream_id, object_kind, object_key, object_name) " +
                "select stream_id, ?, ?, ? from api_ledger.api_stream where stream_code = ?")) {
            for (int i = 1; i <= totalWrites; i++) {
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

            int start = workerIndex * commitsPerWorker + 1;
            int end = start + commitsPerWorker;

            for (int i = start; i < end; i++) {
                if (firstFailure.get() != null) {
                    return;
                }
                recordCommit(cx, objectKey(i));
            }
        } catch (Throwable t) {
            if (firstFailure.compareAndSet(null, t)) {
                System.err.println("worker " + workerIndex + " failed: " + t.getMessage());
                t.printStackTrace(System.err);
            }
        }
    }

    private void recordCommit(Connection cx, String objectKey) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select act_id, act_seq, thread_id from api_ledger.record_commit(?, ?, ?, ?, null)")) {
            ps.setString(1, STREAM_CODE);
            ps.setString(2, PARTICIPANT_CODE);
            ps.setString(3, OBJECT_KIND);
            ps.setString(4, objectKey);
            ps.executeQuery().close();
        }
    }

    private void assertActCoverage(Connection cx, int expectedWrites) throws SQLException {
        try (PreparedStatement ps = cx.prepareStatement(
                "select count(*) as c, min(act_seq) as min_seq, max(act_seq) as max_seq from api_ledger.api_act");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            long count = rs.getLong("c");
            long min = rs.getLong("min_seq");
            long max = rs.getLong("max_seq");

            if (count != expectedWrites) {
                throw new IllegalStateException("expected " + expectedWrites + " acts but found " + count);
            }
            if (expectedWrites > 0 && min != 1) {
                throw new IllegalStateException("expected min act_seq=1 but found " + min);
            }
            if (expectedWrites > 0 && max != expectedWrites) {
                throw new IllegalStateException("expected max act_seq=" + expectedWrites + " but found " + max);
            }
        }
    }

    private void dumpActSeqSample() {
        try (Connection cx = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement ps = cx.prepareStatement(
                     "select act_seq from api_ledger.api_act order by act_seq limit 50");
             ResultSet rs = ps.executeQuery()) {
            List<Long> seqs = new ArrayList<>();
            while (rs.next()) {
                seqs.add(rs.getLong(1));
            }
            System.err.println("first act_seq rows: " + seqs);
        } catch (SQLException e) {
            System.err.println("failed to dump act_seq sample: " + e.getMessage());
        }
    }

    private void initializeConnection(Connection cx) throws SQLException {
        cx.setAutoCommit(true);
        try (Statement st = cx.createStatement()) {
            st.execute("set search_path = api_ledger");
        }
    }

    private static String objectKey(int index) {
        return String.format("EP_%06d", index);
    }
}
