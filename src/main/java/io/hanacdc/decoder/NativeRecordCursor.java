package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;

import java.io.IOException;

/** Ordered, bounded cursor over native HANA redo records. */
public interface NativeRecordCursor extends AutoCloseable {
    boolean hasNext() throws IOException;

    NativeRecordHeader next() throws IOException;

    byte[] readPayload(NativeRecordHeader record) throws IOException;

    @Override
    void close() throws IOException;
}
