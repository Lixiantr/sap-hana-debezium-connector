package io.hanacdc.connect;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A single-slot, explicitly bounded prefetch stage owned by one caller thread. */
final class BoundedPrefetch<T> implements AutoCloseable {
    @FunctionalInterface
    interface Reader<T> {
        T read() throws IOException;
    }

    private final ExecutorService executor;
    private Future<T> pending;
    private boolean closed;
    private long completed;
    private long waitNanos;

    BoundedPrefetch(String threadNamePrefix) {
        Objects.requireNonNull(threadNamePrefix, "threadNamePrefix");
        AtomicInteger sequence = new AtomicInteger();
        executor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(
                    task, threadNamePrefix + "-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    T takeOrRead(Reader<T> reader) throws IOException, InterruptedException {
        Objects.requireNonNull(reader, "reader");
        Future<T> current = pending;
        if (current == null) {
            if (closed) return null;
            return reader.read();
        }
        long started = System.nanoTime();
        try {
            T result = current.get();
            if (result != null) completed++;
            return result;
        }
        catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException io) throw io;
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error fatal) throw fatal;
            throw new IOException("bounded prefetch failed", cause);
        }
        catch (CancellationException error) {
            throw new IOException("bounded prefetch was cancelled", error);
        }
        catch (InterruptedException error) {
            current.cancel(true);
            throw error;
        }
        finally {
            waitNanos += System.nanoTime() - started;
            if (pending == current) pending = null;
        }
    }

    void start(Reader<T> reader) {
        Objects.requireNonNull(reader, "reader");
        if (closed || pending != null) return;
        pending = executor.submit(reader::read);
    }

    long completed() {
        return completed;
    }

    long waitNanos() {
        return waitNanos;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        Future<T> current = pending;
        pending = null;
        if (current != null) current.cancel(true);
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }
}
