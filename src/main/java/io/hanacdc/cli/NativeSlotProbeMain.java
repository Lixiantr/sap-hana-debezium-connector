package io.hanacdc.cli;

import io.hanacdc.decoder.NativeRecordReader;
import io.hanacdc.decoder.NativeRowDescriptorLayout;
import io.hanacdc.decoder.NativeVariableSlotLayout;
import io.hanacdc.decoder.ParallelLogDecoder;
import io.hanacdc.decoder.UnifiedTableRowLayout;

import java.nio.file.Path;
import java.util.HexFormat;

/** Prints a bounded fragment of one native descriptor slot. */
public final class NativeSlotProbeMain {
    private NativeSlotProbeMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException(
                    "usage: NativeSlotProbeMain <dir> <first> <last> <tid> <descriptor> <bytes>");
        }
        int descriptorIndex = Integer.parseInt(args[4]);
        int width = Integer.parseInt(args[5]);
        int sequence = 0;
        for (var backup : ParallelLogDecoder.discoverCorpus(
                Path.of(args[0]), Long.parseLong(args[1]), Long.parseLong(args[2]))) {
            try (var cursor = NativeRecordReader.open(backup)) {
                while (cursor.hasNext()) {
                    var record = cursor.next();
                    if (record.tid() != Long.parseLong(args[3])
                            || record.operationCode() != NativeRecordReader.UT_UPSERT) {
                        continue;
                    }
                    byte[] payload = cursor.readPayload(record);
                    var vector = UnifiedTableRowLayout.parse(record.operationCode(), payload)
                            .orElseThrow();
                    var layout = NativeRowDescriptorLayout.parse(vector, payload).orElseThrow();
                    var descriptor = layout.descriptors().get(descriptorIndex);
                    if ((descriptor.typeCode() & 0x0100) != 0) {
                        int variableIndex = 0;
                        for (int index = 0; index < descriptorIndex; index++) {
                            if ((layout.descriptors().get(index).typeCode() & 0x0100) != 0) {
                                variableIndex++;
                            }
                        }
                        var slot = NativeVariableSlotLayout.parse(layout, payload).orElseThrow()
                                .slots().get(variableIndex);
                        byte[] value = slot.data();
                        System.out.printf("sequence=%d position=%d type=0x%06x slot=%d "
                                        + "variable=%d null=%s length=%d bytes=%s%n",
                                ++sequence, record.position(), descriptor.typeCode(),
                                descriptor.slot(), variableIndex, slot.isNull(), value.length,
                                HexFormat.of().formatHex(value, 0, Math.min(width, value.length)));
                    }
                    else {
                        int offset = layout.directValueOffset(descriptorIndex).orElseThrow();
                        if (offset < 0 || offset + width > payload.length) {
                            throw new IllegalStateException("probe range outside payload");
                        }
                        System.out.printf("sequence=%d position=%d type=0x%06x slot=%d bytes=%s%n",
                                ++sequence, record.position(), descriptor.typeCode(),
                                descriptor.slot(),
                                HexFormat.of().formatHex(payload, offset, offset + width));
                    }
                }
            }
        }
    }
}
