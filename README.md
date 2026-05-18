# queue-alternatives

Stage 3 (대기열) 진입을 위해 [ticketing](https://github.com/dongwooooooo/ticketing) §6 에서 비교 검토할 3가지 대기열 구현 대안.

## 진입 배경

[seat-lock-alternatives](https://github.com/dongwooooooo/seat-lock-alternatives) 의 stress-baseline / stress-baseline-deep 측정으로 다음이 입증됨:
- Stage 2 베이스라인 (비관적 락 + partial UNIQUE) 은 race 정합성 OK
- 단, 5종 운영 부하 모드 (풀 고갈 / Deadlock / Lock timeout / Starvation / Connection leak) 에 취약
- → 대기열로 backend 직격 트래픽 차단 필요

## 대안 목록

| 디렉터리 | 큐 종류 | 핵심 |
|---|---|---|
| [queue-a-redis-sorted-set](queue-a-redis-sorted-set/) | Redis ZSET | Lua atomic ZADD/ZRANGE/ZREM, score = timestamp, 멀티 인스턴스 호환 |
| [queue-b-in-process](queue-b-in-process/) | LinkedBlockingQueue | Spring Bean, ConcurrentSkipListMap, 단일 노드 한정 |
| [queue-c-pg-skip-locked](queue-c-pg-skip-locked/) | PG queue table | `SELECT FOR UPDATE SKIP LOCKED` 디스패처 |

## 공통 시나리오

**사용자 입장 풀이**: BTS 콘서트 오픈 11:00:00, 10,000명이 동시에 "예매 시작" 버튼을 누른다. 모든 요청은 backend 좌석 API 에 바로 가지 않고 대기열을 거친다 — 화면에 "내 앞 N명 / 예상 대기 M초" 가 표시되고, 디스패처가 backend 가 받을 수 있는 만큼만(페이싱) 통과시킨다.

- 큐에 10,000 명 enqueue
- 디스패처가 통과 (각 큐별 페이싱·배치 정책 상이)
- 측정: enqueue throughput, admit throughput, p50/p99 enqueue latency, 인프라 의존성

## 측정 결과

| 큐 | enqueue ops/s | admit ops/s | p50 enqueue (ms) | p99 enqueue (ms) | elapsed (ms) | 인프라 의존 | 멀티 인스턴스 | 채택? |
|---|---|---|---|---|---|---|---|---|
| A. Redis ZSET | 28,571 | 1,095 (paced 1000/s) | 0.95 | 3.2 | 9,479 (페이싱 9초 포함) | Redis 추가 | OK | Stage 4 이월 |
| **B. in-process** | **181,818** | **2,000,000** | **0.009** | **1.27** | **60** | 없음 | NO | **Stage 3 채택** |
| C. PG SKIP LOCKED | 14,005 (32 threads) | 81,300 (8 worker × batch 50) | 0 | 16 | 837 | PG (이미 있음) | OK | 후보 (Stage 4) |

### 관찰 포인트

**A. Redis ZSET — 사용자 입장**
- 사용자가 "예매 시작" 클릭 → backend 가 `redis.ZADD waiting:{event} {timestamp} {token}` 호출 → p99 3.2ms 안에 "대기열 등록" 응답. 네트워크 round trip 1회.
- 디스패처가 1초마다 1000개씩 통과시키므로 사용자 화면에 "내 앞 N명" 이 매초 ~1000 줄어든다.
- 한계: TTL 은 admitted SET 키 단위. 토큰별 정확 60초 만료가 불가능 — 일부 사용자는 60.X초까지 살아남을 수 있다. NTP 점프로 score 역전 시 같은 시각 enqueue 한 두 사용자의 순서가 뒤바뀔 수 있음 (엄격 단조성 필요 시 `INCR` 카운터).

**B. in-process — 사용자 입장**
- 사용자가 "예매 시작" 클릭 → Spring Bean 내부 `LinkedBlockingQueue` 에 적재 → p99 1.27ms 안에 응답. 외부 인프라 호출 0회.
- 단일 JVM 안에서 처리되므로 인스턴스 1대 한정. 인스턴스를 2대로 늘리면 두 큐가 분리돼 "내 앞 N명" 이 인스턴스마다 다른 숫자로 표시되는 사고.
- `position()` 은 ConcurrentSkipListMap rank 미지원이라 head 순회 O(n) — 10K 까지는 ms 단위 OK, 100K+ 부터 사용자 화면의 "내 앞 N명" 갱신이 늦어진다.
- TTL 만료는 1초 주기 스케줄러 + lazy check 로 보장.

**C. PG SKIP LOCKED — 사용자 입장**
- 사용자가 "예매 시작" 클릭 → `INSERT INTO waiting_queue` 1발 → enqueue p99 ~16ms. 디스패처 worker 8개가 `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 50` 로 50건씩 묶어 통과.
- 별도 인프라 없이 PG 만으로 동작 — 운영 부담 최소. 그러나 큐 트래픽이 좌석 예매 트래픽과 같은 DB 에 합산되어 베이스라인 한계 영역(풀 고갈/lock timeout)이 더 빨리 노출된다.
- Spring Boot 4.0 + Flyway 11.14 autoconfig 미동작 (호환 이슈) → `spring.sql.init.schema-locations` 우회 필요. **운영 적용 전 호환 문제 해결 필요**.

## 채택 결정

**Stage 3 (단일 인스턴스 포트폴리오) 채택: B (in-process)**

사용자 입장에서 본 채택 이유:
- 사용자가 "예매 시작" 누른 순간 응답 p99 1.27ms — 다른 두 대안의 3~16배 빠르다.
- 외부 인프라 의존이 없어 Redis/PG 장애 시 큐 자체가 죽을 위험이 없다 (단일 JVM 안에서 동작).
- 포트폴리오 단일 인스턴스 가정과 정확히 일치. 단, 인스턴스 2대 이상이면 두 큐가 분리돼 사용자 화면의 "내 앞 N명" 이 인스턴스마다 다른 숫자가 되는 사고 — 멀티 인스턴스로 가는 순간 A/C 로 교체 필수.
- 코드 단순 — Lua atomic 스크립트도 PG batch tuning 도 없음.

**Stage 4 (멀티 인스턴스 확장) 이월: A 또는 C**

- A (Redis ZSET): 별도 Redis 가 이미 있다면 깔끔. Lua atomic 으로 multi-node 사용자 동시 admit 안전.
- C (PG SKIP LOCKED): 별도 인프라 없음. 단, PG 가 이미 좌석 예매 트래픽으로 부하 한계인데 큐 트래픽까지 합산되므로 좌석 예매 사용자가 영향받지 않는지 측정·검증 필수. Spring Boot 4 호환 이슈 선결.

Stage 3 → Stage 4 전환 시 `WaitingQueue` 인터페이스만 유지하고 impl 만 교체. 본 레포의 세 모듈 모두 같은 인터페이스를 따른다.

## 실행

```bash
./gradlew :queue-a-redis-sorted-set:test
./gradlew :queue-b-in-process:test
./gradlew :queue-c-pg-skip-locked:test
./gradlew test  # 전체
```
