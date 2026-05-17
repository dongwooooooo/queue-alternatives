package com.dongwoo.queuea;

import static org.assertj.core.api.Assertions.assertThat;

import com.dongwoo.queuea.queue.RedisWaitingQueue;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class QueueAThroughputTest {

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", TestcontainersConfiguration.REDIS::getHost);
        registry.add("spring.data.redis.port", () -> TestcontainersConfiguration.REDIS.getMappedPort(6379));
    }

    private static final int ENQUEUE_COUNT = 10_000;
    private static final int THREAD_COUNT = 32;
    private static final int DISPATCH_BATCH = 1_000;

    @Autowired
    private RedisWaitingQueue queue;

    @BeforeEach
    void reset() {
        // RedisTemplate 직접 호출 — @Transactional 은 self-invocation 으로 proxy 안 됨
        queue.reset();
    }

    @Test
    void throughput_10k_enqueue_then_dispatch_1k_per_second() throws Exception {
        // ---- 1. enqueue 단계 ----
        // ExecutorService 는 반드시 newFixedThreadPool(threadCount) — 작게 잡으면
        // CountDownLatch ready.await() 에서 deadlock 발생.
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch ready = new CountDownLatch(THREAD_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(ENQUEUE_COUNT);

        List<Long> latenciesNs = Collections.synchronizedList(new ArrayList<>(ENQUEUE_COUNT));
        List<String> tokens = Collections.synchronizedList(new ArrayList<>(ENQUEUE_COUNT));

        int perThread = ENQUEUE_COUNT / THREAD_COUNT;
        int remainder = ENQUEUE_COUNT % THREAD_COUNT;

        long enqueueStartWall = 0L;
        for (int t = 0; t < THREAD_COUNT; t++) {
            int count = perThread + (t < remainder ? 1 : 0);
            int offset = t * perThread + Math.min(t, remainder);
            pool.submit(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < count; i++) {
                    String userId = "user-" + (offset + i);
                    long t0 = System.nanoTime();
                    String token = queue.enqueue(userId);
                    long t1 = System.nanoTime();
                    latenciesNs.add(t1 - t0);
                    tokens.add(token);
                    done.countDown();
                }
            });
        }
        ready.await();
        enqueueStartWall = System.currentTimeMillis();
        long enqueueStartNs = System.nanoTime();
        start.countDown();
        done.await(2, TimeUnit.MINUTES);
        long enqueueElapsedMs = (System.nanoTime() - enqueueStartNs) / 1_000_000L;
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(tokens).hasSize(ENQUEUE_COUNT);
        assertThat(queue.waitingCount()).isEqualTo(ENQUEUE_COUNT);

        // 첫 토큰의 position 확인 (sanity check)
        long firstPos = queue.position(tokens.get(0));
        assertThat(firstPos).isPositive();

        // ---- 2. dispatcher: 초당 1000명 통과 ----
        long admitStartNs = System.nanoTime();
        int admittedTotal = 0;
        int rounds = ENQUEUE_COUNT / DISPATCH_BATCH;
        for (int r = 0; r < rounds; r++) {
            queue.admitNext(DISPATCH_BATCH);
            admittedTotal += DISPATCH_BATCH;
            // 라운드당 1초 페이싱 — 단, 마지막 라운드는 페이싱 생략
            if (r < rounds - 1) {
                Thread.sleep(1_000);
            }
        }
        long admitElapsedMs = (System.nanoTime() - admitStartNs) / 1_000_000L;

        assertThat(queue.waitingCount()).isEqualTo(0L);
        assertThat(queue.admittedCount()).isEqualTo(ENQUEUE_COUNT);
        // 통과된 토큰의 isAdmitted 검증
        assertThat(queue.isAdmitted(tokens.get(0))).isTrue();
        assertThat(queue.isAdmitted(tokens.get(ENQUEUE_COUNT - 1))).isTrue();

        // ---- 3. 측정 ----
        long[] sorted = latenciesNs.stream().mapToLong(Long::longValue).sorted().toArray();
        double p50Ms = percentileMs(sorted, 0.50);
        double p99Ms = percentileMs(sorted, 0.99);
        long enqueueOps = enqueueElapsedMs > 0 ? (long) (ENQUEUE_COUNT * 1000L / enqueueElapsedMs) : ENQUEUE_COUNT;
        long admitOps = admitElapsedMs > 0 ? (long) (admittedTotal * 1000L / admitElapsedMs) : admittedTotal;
        long totalElapsedMs = enqueueElapsedMs + admitElapsedMs;

        PrintStream out = System.out;
        out.println("===== QUEUE-A RESULT =====");
        out.println("enqueueCount=" + ENQUEUE_COUNT);
        out.println("admittedCount=" + admittedTotal);
        out.println("elapsedMs=" + totalElapsedMs);
        out.println("enqueueElapsedMs=" + enqueueElapsedMs);
        out.println("admitElapsedMs=" + admitElapsedMs);
        out.println("enqueueOpsPerSec=" + enqueueOps);
        out.println("admitOpsPerSec=" + admitOps);
        out.println("throughput=" + enqueueOps);
        out.println("p50EnqueueMs=" + String.format("%.3f", p50Ms));
        out.println("p99EnqueueMs=" + String.format("%.3f", p99Ms));
        out.println("p99=" + String.format("%.3f", p99Ms));
        out.println("success=true");
        out.println("==========================");
    }

    private static double percentileMs(long[] sortedNs, double p) {
        if (sortedNs.length == 0) return 0.0;
        int idx = (int) Math.ceil(p * sortedNs.length) - 1;
        if (idx < 0) idx = 0;
        if (idx >= sortedNs.length) idx = sortedNs.length - 1;
        return sortedNs[idx] / 1_000_000.0;
    }
}
