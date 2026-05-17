package com.dongwoo.queuec.queue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/**
 * PostgreSQL SELECT FOR UPDATE SKIP LOCKED 기반 대기열.
 *
 * - enqueue: 단순 INSERT (UNIQUE 토큰).
 * - admitNext: SKIP LOCKED 로 동시에 여러 워커가 안전하게 통과 처리.
 *   동일 row 잠금 충돌을 피하면서 처리량 확보.
 */
@Component
public class PgWaitingQueue implements WaitingQueue {

    private final JdbcTemplate jdbc;

    public PgWaitingQueue(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public String enqueue(String userId) {
        String token = UUID.randomUUID().toString();
        jdbc.update(
                "INSERT INTO waiting_queue (token, user_id) VALUES (?, ?)",
                token, userId
        );
        return token;
    }

    @Override
    public long position(String token) {
        // 자기 seq 보다 작은 WAITING row 수 + 1.
        // 본인이 이미 ADMITTED 면 0 반환.
        Long pos = jdbc.queryForObject(
                """
                SELECT CASE
                    WHEN me.status = 'ADMITTED' THEN 0
                    ELSE COALESCE((
                        SELECT count(*) FROM waiting_queue w
                         WHERE w.status = 'WAITING' AND w.seq < me.seq
                    ), 0) + 1
                END
                FROM waiting_queue me WHERE me.token = ?
                """,
                Long.class, token
        );
        return pos == null ? -1L : pos;
    }

    @Override
    public int admitNext(int n) {
        if (n <= 0) {
            return 0;
        }
        // SKIP LOCKED 로 다른 트랜잭션이 잡고 있는 row 건너뛰며 상위 n 건 UPDATE.
        List<Long> admittedSeqs = jdbc.queryForList(
                """
                UPDATE waiting_queue
                   SET status = 'ADMITTED', admitted_at = now()
                 WHERE seq IN (
                    SELECT seq FROM waiting_queue
                     WHERE status = 'WAITING'
                     ORDER BY seq
                     FOR UPDATE SKIP LOCKED
                     LIMIT ?
                 )
                 RETURNING seq
                """,
                Long.class, n
        );
        return admittedSeqs.size();
    }

    @Override
    public boolean isAdmitted(String token) {
        Integer cnt = jdbc.queryForObject(
                "SELECT count(*) FROM waiting_queue WHERE token = ? AND status = 'ADMITTED'",
                Integer.class, token
        );
        return cnt != null && cnt > 0;
    }
}
