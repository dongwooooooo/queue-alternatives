package com.dongwoo.queuec;

import com.dongwoo.queuec.queue.WaitingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class QueueCThroughputTest {

    private static final int ENQUEUE_COUNT = 10_000;
    private static final int THREAD_COUNT = 32;
    private static final int ADMIT_WORKERS = 8;
    private static final int ADMIT_BATCH = 50;

    @Autowired
    private WaitingQueue queue;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void reset() {
        // self-invocation 회피 위해 @Transactional 안 쓰고 직접 TRUNCATE.
        jdbc.execute("TRUNCATE TABLE waiting_queue RESTART IDENTITY");
    }

    @Test
    void enqueueAndAdmitThroughput() throws Exception {
        // ===== enqueue 단계 =====
        ExecutorService enqExec = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch enqStart = new CountDownLatch(1);
        CountDownLatch enqDone = new CountDownLatch(ENQUEUE_COUNT);
        List<String> tokens = new CopyOnWriteArrayList<>();
        List<Long> enqLatenciesNanos = Collections.synchronizedList(new java.util.ArrayList<>(ENQUEUE_COUNT));

        long enqStartTs = System.nanoTime();
        for (int i = 0; i < ENQUEUE_COUNT; i++) {
            final int idx = i;
            enqExec.submit(() -> {
                try {
                    enqStart.await();
                    long t0 = System.nanoTime();
                    String token = queue.enqueue("user-" + idx);
                    long t1 = System.nanoTime();
                    tokens.add(token);
                    enqLatenciesNanos.add(t1 - t0);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    enqDone.countDown();
                }
            });
        }
        enqStart.countDown();
        boolean enqOk = enqDone.await(120, TimeUnit.SECONDS);
        long enqElapsedNanos = System.nanoTime() - enqStartTs;
        enqExec.shutdown();
        assertThat(enqOk).as("enqueue 완료 대기").isTrue();
        assertThat(tokens).hasSize(ENQUEUE_COUNT);

        // ===== admit 단계 =====
        ExecutorService admitExec = Executors.newFixedThreadPool(ADMIT_WORKERS);
        CountDownLatch admitStart = new CountDownLatch(1);
        AtomicInteger admittedTotal = new AtomicInteger();
        CountDownLatch admitDone = new CountDownLatch(ADMIT_WORKERS);

        long admitStartTs = System.nanoTime();
        for (int w = 0; w < ADMIT_WORKERS; w++) {
            admitExec.submit(() -> {
                try {
                    admitStart.await();
                    while (admittedTotal.get() < ENQUEUE_COUNT) {
                        int got = queue.admitNext(ADMIT_BATCH);
                        if (got == 0) {
                            // 다른 워커가 모두 들고감 — 짧게 양보.
                            Thread.sleep(1);
                            // 다 끝났는지 재확인 (DB count 보고 종료)
                            Integer remaining = jdbc.queryForObject(
                                    "SELECT count(*) FROM waiting_queue WHERE status='WAITING'",
                                    Integer.class);
                            if (remaining == null || remaining == 0) {
                                break;
                            }
                        } else {
                            admittedTotal.addAndGet(got);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    admitDone.countDown();
                }
            });
        }
        admitStart.countDown();
        boolean admitOk = admitDone.await(180, TimeUnit.SECONDS);
        long admitElapsedNanos = System.nanoTime() - admitStartTs;
        admitExec.shutdown();
        assertThat(admitOk).as("admit 완료 대기").isTrue();

        // 최종 ADMITTED row 수로 정합성 검증.
        Integer admittedRows = jdbc.queryForObject(
                "SELECT count(*) FROM waiting_queue WHERE status='ADMITTED'", Integer.class);
        assertThat(admittedRows).isEqualTo(ENQUEUE_COUNT);

        // 샘플 검증: 첫 토큰 isAdmitted true.
        assertThat(queue.isAdmitted(tokens.get(0))).isTrue();

        long enqElapsedMs = enqElapsedNanos / 1_000_000L;
        long admitElapsedMs = admitElapsedNanos / 1_000_000L;
        long totalElapsedMs = enqElapsedMs + admitElapsedMs;

        double enqueueOpsPerSec = ENQUEUE_COUNT * 1000.0 / Math.max(enqElapsedMs, 1);
        double admitOpsPerSec = ENQUEUE_COUNT * 1000.0 / Math.max(admitElapsedMs, 1);

        // 백분위 계산
        List<Long> sorted = new java.util.ArrayList<>(enqLatenciesNanos);
        Collections.sort(sorted);
        long p50Ns = sorted.get((int) (sorted.size() * 0.50));
        long p99Ns = sorted.get((int) (sorted.size() * 0.99));
        long p50Ms = p50Ns / 1_000_000L;
        long p99Ms = p99Ns / 1_000_000L;

        System.out.println("===== QUEUE-C RESULT =====");
        System.out.println("enqueueCount=" + ENQUEUE_COUNT);
        System.out.println("admittedCount=" + admittedRows);
        System.out.println("elapsedMs=" + totalElapsedMs);
        System.out.println("enqueueOpsPerSec=" + (long) enqueueOpsPerSec);
        System.out.println("admitOpsPerSec=" + (long) admitOpsPerSec);
        System.out.println("p50EnqueueMs=" + p50Ms);
        System.out.println("p99EnqueueMs=" + p99Ms);
        System.out.println("==========================");
    }
}
