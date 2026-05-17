package com.dongwoo.queuea.queue;

/**
 * 대기열 인터페이스.
 *
 * position() 반환 규칙:
 * - 1 이상: 현재 대기 순번 (1-based)
 * - {@link #ALREADY_ADMITTED}: 이미 통과된 토큰
 * - {@link #NOT_FOUND}: 존재하지 않는 토큰
 */
public interface WaitingQueue {

    long ALREADY_ADMITTED = -1L;
    long NOT_FOUND = -2L;

    /** 사용자 ID에 대해 새 토큰을 발급하고 큐에 enqueue. 반환값은 토큰. */
    String enqueue(String userId);

    /** 토큰의 현재 큐 순번 (1-based). */
    long position(String token);

    /** 큐 앞에서 n명을 통과 처리. 통과시킨 실제 수 만큼 true 횟수. */
    boolean admitNext(int n);

    /** 토큰이 통과되었는지 여부. */
    boolean isAdmitted(String token);
}
