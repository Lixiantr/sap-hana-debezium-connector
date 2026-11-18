package io.hanacdc.connect;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BoundedPrefetchTest {
    @Test
    void holdsAtMostOnePrefetchedValue() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        try (BoundedPrefetch<Integer> prefetch = new BoundedPrefetch<>("bounded-test")) {
            prefetch.start(reads::incrementAndGet);
            prefetch.start(reads::incrementAndGet);

            assertEquals(1, prefetch.takeOrRead(reads::incrementAndGet));
            assertEquals(1, reads.get());
            assertEquals(1, prefetch.completed());
        }
    }

    @Test
    void preservesReaderFailureTypeAndMessage() {
        try (BoundedPrefetch<Integer> prefetch = new BoundedPrefetch<>("failure-test")) {
            prefetch.start(() -> {
                throw new IOException("native read failed");
            });

            IOException failure = assertThrows(IOException.class,
                    () -> prefetch.takeOrRead(() -> 1));
            assertEquals("native read failed", failure.getMessage());
        }
    }

    @Test
    void closeInterruptsBlockedReader() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        BoundedPrefetch<Integer> prefetch = new BoundedPrefetch<>("interrupt-test");
        prefetch.start(() -> {
            entered.countDown();
            try {
                Thread.sleep(TimeUnit.MINUTES.toMillis(1));
            }
            catch (InterruptedException error) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
            return null;
        });
        assertTrue(entered.await(1, TimeUnit.SECONDS));

        prefetch.close();

        assertTrue(interrupted.get());
    }
}
