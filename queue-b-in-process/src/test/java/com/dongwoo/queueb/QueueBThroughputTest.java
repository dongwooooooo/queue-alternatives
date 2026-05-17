package com.dongwoo.queueb;

import com.dongwoo.queueb.queue.InProcessWaitingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class QueueBThroughputTest {

    private static final int USER_COUNT = 10_000;
    private static final int THREAD_COUNT = 32;
    private static final int ADMIT_BATCH = 200;

    @Autowired
    InProcessWaitingQueue queue;

    @BeforeEach
    void reset() {
        queue.clear();
    }

    @Test
    void enqueue10kAndAdmitAll() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(USER_COUNT);
        ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> tokens = new ConcurrentLinkedQueue<>();
        AtomicInteger errors = new AtomicInteger();

        long t0 = System.nanoTime();

        for (int i = 0; i < USER_COUNT; i++) {
            final int idx = i;
            pool.submit(() -> {
                try {
                    start.await();
                    long s = System.nanoTime();
                    String token = queue.enqueue("user-" + idx);
                    long e = System.nanoTime();
                    tokens.add(token);
                    latencies.add(TimeUnit.NANOSECONDS.toMicros(e - s));
                } catch (Exception ex) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        boolean finished = done.await(60, TimeUnit.SECONDS);
        long enqueueElapsedNanos = System.nanoTime() - t0;

        assertThat(finished).as("enqueue 작업이 60초 안에 끝나야 함").isTrue();
        assertThat(errors.get()).isZero();
        assertThat(tokens.size()).isEqualTo(USER_COUNT);
        assertThat(queue.waitingSize()).isEqualTo(USER_COUNT);

        // position 동작 sanity check (전체 순회 대신 일부만)
        // 단일 토큰 위치는 1~USER_COUNT 사이.
        String anyToken = tokens.peek();
        long pos = queue.position(anyToken);
        assertThat(pos).isBetween(1L, (long) USER_COUNT);

        // dispatcher: 배치로 admit
        long admitStart = System.nanoTime();
        int admittedTotal = 0;
        while (queue.waitingSize() > 0) {
            queue.admitNext(ADMIT_BATCH);
            admittedTotal = USER_COUNT - queue.waitingSize();
        }
        long admitElapsedNanos = System.nanoTime() - admitStart;

        assertThat(queue.admittedSize()).isEqualTo(USER_COUNT);

        // isAdmitted 검증 (랜덤 샘플)
        List<String> sample = new ArrayList<>(tokens);
        Collections.shuffle(sample);
        for (int i = 0; i < 100; i++) {
            assertThat(queue.isAdmitted(sample.get(i))).isTrue();
        }

        pool.shutdownNow();

        // 측정 출력
        long enqueueMs = TimeUnit.NANOSECONDS.toMillis(enqueueElapsedNanos);
        long admitMs = TimeUnit.NANOSECONDS.toMillis(admitElapsedNanos);
        long totalMs = enqueueMs + admitMs;
        double enqueueOps = enqueueMs > 0 ? USER_COUNT * 1000.0 / enqueueMs : -1;
        double admitOps = admitMs > 0 ? USER_COUNT * 1000.0 / admitMs : -1;

        List<Long> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        long p50us = sorted.get((int) (sorted.size() * 0.50));
        long p99us = sorted.get((int) (sorted.size() * 0.99));

        System.out.println("===== QUEUE-B RESULT =====");
        System.out.println("enqueueCount=" + USER_COUNT);
        System.out.println("admittedCount=" + queue.admittedSize());
        System.out.println("elapsedMs=" + totalMs);
        System.out.println("enqueueOpsPerSec=" + (long) enqueueOps);
        System.out.println("admitOpsPerSec=" + (long) admitOps);
        System.out.println("p50EnqueueMs=" + (p50us / 1000.0));
        System.out.println("p99EnqueueMs=" + (p99us / 1000.0));
        System.out.println("==========================");
    }
}
