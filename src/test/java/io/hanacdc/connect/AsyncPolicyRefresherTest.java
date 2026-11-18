package io.hanacdc.connect;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AsyncPolicyRefresherTest {
    @Test
    void schedulesWithoutBlockingAndReturnsCompletedValue() throws Exception {
        AtomicLong clock = new AtomicLong();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (var refresher = new AsyncPolicyRefresher<String>(
                10, 100, clock::get, Executors.newSingleThreadExecutor())) {
            clock.set(10);
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(1), () -> assertTrue(refresher.poll(() -> () -> {
                        started.countDown();
                        release.await();
                        return "ready";
                    }).isEmpty()));
            assertTrue(started.await(1, TimeUnit.SECONDS));
            release.countDown();

            AsyncPolicyRefresher.Completion<String> completed = awaitCompletion(
                    refresher, () -> () -> "unexpected");
            assertEquals("ready", completed.value());
            assertEquals(0, completed.elapsedNanos());
        }
    }

    @Test
    void propagatesBackgroundFailure() throws Exception {
        AtomicLong clock = new AtomicLong(10);
        try (var refresher = new AsyncPolicyRefresher<String>(
                10, 100, clock::get, Executors.newSingleThreadExecutor())) {
            clock.set(20);
            refresher.poll(() -> () -> {
                throw new SQLException("catalog unavailable");
            });
            ExecutionException error = awaitFailure(
                    refresher, () -> () -> "unexpected");
            assertTrue(error.getCause() instanceof SQLException);
        }
    }

    @Test
    void cancelsHungRefreshWhenLastSuccessBecomesStale() throws Exception {
        AtomicLong clock = new AtomicLong();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try (var refresher = new AsyncPolicyRefresher<String>(
                10, 100, clock::get, Executors.newSingleThreadExecutor())) {
            clock.set(10);
            refresher.poll(() -> () -> {
                started.countDown();
                try {
                    new CountDownLatch(1).await();
                }
                catch (InterruptedException error) {
                    interrupted.countDown();
                    throw error;
                }
                return "unreachable";
            });
            assertTrue(started.await(1, TimeUnit.SECONDS));
            clock.set(101);
            var error = assertThrows(AsyncPolicyRefresher.StaleResultException.class,
                    () -> refresher.poll(() -> () -> "unexpected"));
            assertEquals(101, error.ageNanos());
            assertEquals(100, error.maximumNanos());
            assertTrue(interrupted.await(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectsStalenessBelowRefreshInterval() {
        var executor = Executors.newSingleThreadExecutor();
        try {
            assertThrows(IllegalArgumentException.class, () ->
                    new AsyncPolicyRefresher<String>(10, 9, System::nanoTime, executor));
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void closeCancelsActiveRefresh() throws Exception {
        AtomicLong clock = new AtomicLong();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        var refresher = new AsyncPolicyRefresher<String>(
                10, 100, clock::get, Executors.newSingleThreadExecutor());
        clock.set(10);
        refresher.poll(() -> () -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            }
            catch (InterruptedException error) {
                interrupted.countDown();
                throw error;
            }
            return "unreachable";
        });
        assertTrue(started.await(1, TimeUnit.SECONDS));

        refresher.close();

        assertFalse(refresher.active());
        assertTrue(interrupted.await(1, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class,
                () -> refresher.poll(() -> () -> "unexpected"));
    }

    private static <T> AsyncPolicyRefresher.Completion<T> awaitCompletion(
            AsyncPolicyRefresher<T> refresher,
            AsyncPolicyRefresher.JobFactory<T> jobs) throws Exception {
        for (int attempt = 0; attempt < 1_000; attempt++) {
            Optional<AsyncPolicyRefresher.Completion<T>> value = refresher.poll(jobs);
            if (value.isPresent()) {
                return value.orElseThrow();
            }
            Thread.sleep(1);
        }
        throw new AssertionError("background refresh did not complete");
    }

    private static <T> ExecutionException awaitFailure(
            AsyncPolicyRefresher<T> refresher,
            AsyncPolicyRefresher.JobFactory<T> jobs) throws Exception {
        for (int attempt = 0; attempt < 1_000; attempt++) {
            try {
                refresher.poll(jobs);
            }
            catch (ExecutionException error) {
                return error;
            }
            Thread.sleep(1);
        }
        throw new AssertionError("background refresh failure was not observed");
    }
}
