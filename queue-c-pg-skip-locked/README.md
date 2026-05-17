# queue-c-pg-skip-locked

PostgreSQL `SELECT FOR UPDATE SKIP LOCKED` 기반 대기열 구현.

## 동작

- `enqueue`: `waiting_queue` 테이블에 토큰(UUID)과 user_id를 INSERT.
- `position`: 자기 seq 보다 작은 WAITING row 수 + 1 (ADMITTED 면 0).
- `admitNext(n)`: 상위 n 건을 `FOR UPDATE SKIP LOCKED` 로 잠그며 ADMITTED 로 갱신.
- `isAdmitted`: token 의 status 가 ADMITTED 인지 확인.

## 실행

```bash
../gradlew :queue-c-pg-skip-locked:test
```

Testcontainers 가 `postgres:16` 컨테이너를 띄우고 Flyway 가 V1__init.sql 을 적용한다.

## 측정 출력

```
===== QUEUE-C RESULT =====
enqueueCount=10000
admittedCount=10000
elapsedMs=<n>
enqueueOpsPerSec=<n>
admitOpsPerSec=<n>
p50EnqueueMs=<n>
p99EnqueueMs=<n>
==========================
```
