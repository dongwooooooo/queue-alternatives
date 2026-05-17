package com.dongwoo.queuea;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Redis 7 GenericContainer 를 JVM 라이프사이클에 묶어 1회만 시작.
 *
 * Testcontainers redis 모듈 미사용 — generic container 로 6379 노출.
 * ServiceConnection 미사용 — host/port 를 {@link DynamicPropertySource} 로 주입.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static {
        REDIS.start();
        // JVM 종료 시 컨테이너 정지. Ryuk 비활성 환경에서 안전.
        Runtime.getRuntime().addShutdownHook(new Thread(REDIS::stop));
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
}
