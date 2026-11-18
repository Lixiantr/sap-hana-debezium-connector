package io.hanacdc.connect;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/** Runs one control-plane refresh at a time without blocking the source data path. */
final class AsyncPolicyRefresher<T> implements AutoCloseable {
    @FunctionalInterface
    interface JobFactory<T> {
        Callable<T> create();
    }

    record Completion<T>(T value, long elapsedNanos) {}

    static final class StaleResultException extends IllegalStateException {
        private final long ageNanos;
        private final long maximumNanos;

        StaleResultException(long ageNanos, long maximumNanos) {
            super("FATAL_CONTROL_PLANE_STALE: age_nanos=" + ageNanos
                    + " maximum_nanos=" + maximumNanos);
            this.ageNanos = ageNanos;
            this.maximumNanos = maximumNanos;
        }

        long ageNanos() {
            return ageNanos;
        }

        long maximumNanos() {
            return maximumNanos;
        }
    }

    private record TimedValue<T>(T value, long elapsedNanos) {}

    private static final AtomicInteger THREAD_SEQUENCE = new AtomicInteger();

    private final long intervalNanos;
    private final long maximumStalenessNanos;
    private final LongSupplier nanoClock;
    private final ExecutorService executor;
    private Future<TimedValue<T>> active;
    private long lastStartedNanos;
    private long lastSuccessNanos;
    private boolean closed;

    static <T> AsyncPolicyRefresher<T> create(
            long intervalMillis, long maximumStalenessMillis) {
        ThreadFactory threads = runnable -> {
            Thread thread = new Thread(runnable,
                    "hana-cdc-policy-refresh-" + THREAD_SEQUENCE.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return new AsyncPolicyRefresher<>(
                TimeUnit.MILLISECONDS.toNanos(intervalMillis),
                TimeUnit.MILLISECONDS.toNanos(maximumStalenessMillis),
                System::nanoTime,
                Executors.newSingleThreadExecutor(threads));
    }

    AsyncPolicyRefresher(
            long intervalNanos,
            long maximumStalenessNanos,
            LongSupplier nanoClock,
            ExecutorService executor) {
        if (intervalNanos <= 0 || maximumStalenessNanos < intervalNanos) {
            throw new IllegalArgumentException(
                    "maximum staleness must be greater than or equal to refresh interval");
        }
        this.intervalNanos = intervalNanos;
        this.maximumStalenessNanos = maximumStalenessNanos;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.executor = Objects.requireNonNull(executor, "executor");
        long now = nanoClock.getAsLong();
        lastStartedNanos = now;
        lastSuccessNanos = now;
    }

    Optional<Completion<T>> poll(JobFactory<T> jobs)
            throws InterruptedException, ExecutionException {
        if (closed) {
            throw new IllegalStateException("policy refresher is closed");
        }
        Objects.requireNonNull(jobs, "jobs");
        long now = nanoClock.getAsLong();
        Optional<Completion<T>> completion = Optional.empty();
        if (active != null && active.isDone()) {
            Future<TimedValue<T>> finished = active;
            active = null;
            TimedValue<T> value = finished.get();
            lastSuccessNanos = now;
            completion = Optional.of(new Completion<>(value.value(), value.elapsedNanos()));
        }

        long age = now - lastSuccessNanos;
        if (age > maximumStalenessNanos) {
            if (active != null) {
                active.cancel(true);
                active = null;
            }
            throw new StaleResultException(age, maximumStalenessNanos);
        }

        if (active == null && now - lastStartedNanos >= intervalNanos) {
            Callable<T> job = Objects.requireNonNull(jobs.create(), "refresh job");
            lastStartedNanos = now;
            active = executor.submit(() -> {
                long started = nanoClock.getAsLong();
                T value = job.call();
                return new TimedValue<>(value, nanoClock.getAsLong() - started);
            });
        }
        return completion;
    }

    boolean active() {
        return active != null;
    }

    @Override
    public void close() {
        closed = true;
        if (active != null) {
            active.cancel(true);
            active = null;
        }
        executor.shutdownNow();
    }
}
