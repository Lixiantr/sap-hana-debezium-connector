package io.hanacdc.decoder;

import io.hanacdc.decoder.NativeRecordReader.NativeRecordHeader;
import io.hanacdc.decoder.ParallelLogDecoder.BackupFile;
import io.hanacdc.schema.TableDecodePlan;
import io.hanacdc.schema.TableMetadata;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Types;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NativeContainerResolverTest {
    @Test
    void resolvesCatalogContainerWhenRedoPoidDiffersFromTableOid() {
        TableDecodePlan plan = plan(165898, 0xfe0000017dL);
        NativeContainerResolver resolver = new NativeContainerResolver(List.of(plan));

        var binding = resolver.observe(record(337217991, 0xfe0000017dL)).orElseThrow();

        assertEquals(165898, binding.plan().metadata().objectId());
        assertEquals(0xfe0000017dL, binding.containerId());
    }

    @Test
    void pinsByPoidAndRejectsUnbarrieredContainerChange() {
        TableDecodePlan plan = plan(337217991, 0);
        NativeContainerResolver resolver = new NativeContainerResolver(List.of(plan));

        var binding = resolver.observe(record(337217991, 0xfe0000017dL)).orElseThrow();
        assertEquals(plan.schemaFingerprint(), binding.plan().schemaFingerprint());
        assertEquals(binding, resolver.byContainer(0xfe0000017dL).orElseThrow());

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> resolver.observe(record(337217991, 0xfe0000017eL)));
        assertTrue(error.getMessage().contains("FATAL_TABLE_BINDING"));
    }

    @Test
    void advancesContainerOnlyThroughExplicitBarrier() {
        NativeContainerResolver resolver = new NativeContainerResolver(
                List.of(plan(337217991, 0xfe0000017dL)));

        var next = resolver.advanceGeneration(337217991, 0xfe0000017eL);

        assertEquals(1, next.generation());
        assertTrue(resolver.byContainer(0xfe0000017dL).isEmpty());
        assertEquals(next, resolver.byContainer(0xfe0000017eL).orElseThrow());
    }

    @Test
    void resolvesEveryLeafPartitionToTheSameLogicalTable() {
        TableMetadata base = plan(165898, 0xfe0000017dL).metadata();
        TableDecodePlan partitioned = TableDecodePlan.compile(new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(), base.objectId(),
                List.of(
                        new TableMetadata.Partition(
                                1, 0xfe0000017dL, "hana01", 39040, 2, "indexserver"),
                        new TableMetadata.Partition(
                                2, 0xfe0000017eL, "hana01", 39040, 2, "indexserver")),
                false, base.columns(), base.auxiliaryColumns(),
                base.primaryKeyColumns()));
        NativeContainerResolver resolver = new NativeContainerResolver(List.of(partitioned));

        var first = resolver.observe(record(1, 0xfe0000017dL)).orElseThrow();
        var second = resolver.observe(record(2, 0xfe0000017eL)).orElseThrow();

        assertEquals(first.plan(), second.plan());
        assertEquals(1, first.partId());
        assertEquals(2, second.partId());
    }

    @Test
    void rejectsUnknownContainerForCataloguedPartitionedTable() {
        TableMetadata base = plan(165898, 0xfe0000017dL).metadata();
        TableDecodePlan partitioned = TableDecodePlan.compile(new TableMetadata(
                base.schemaName(), base.tableName(), base.storageType(), base.objectId(),
                List.of(
                        new TableMetadata.Partition(
                                1, 0xfe0000017dL, "hana01", 39040, 2, "indexserver"),
                        new TableMetadata.Partition(
                                2, 0xfe0000017eL, "hana01", 39040, 2, "indexserver")),
                false, base.columns(), base.auxiliaryColumns(),
                base.primaryKeyColumns()));
        NativeContainerResolver resolver = new NativeContainerResolver(List.of(partitioned));

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> resolver.observe(record(165898, 0xfe0000017fL)));
        assertTrue(error.getMessage().contains("FATAL_TABLE_BINDING"));
    }

    private static TableDecodePlan plan(long objectId, long containerId) {
        return TableDecodePlan.compile(new TableMetadata("CDC", "T", "COLUMN",
                objectId, containerId,
                List.of(
                        new TableMetadata.Column(1, 201, "ID", "BIGINT", Types.BIGINT,
                                8, 19, 0, false, false, false, ""),
                        new TableMetadata.Column(2, 202, "IS_DELETED", "BOOLEAN", Types.BOOLEAN,
                                1, 0, 0, false, false, false, "FALSE")),
                List.of(),
                List.of("ID")));
    }

    private static NativeRecordHeader record(long poid, long container) {
        BackupFile backup = new BackupFile(Path.of("log_backup_2_0_1_2.3"),
                2, 0, 1, 2, 3);
        return new NativeRecordHeader(backup, 4096, 1, 7,
                NativeRecordReader.OUTER_UNIFIED_TABLE, 64, 128,
                poid, container, NativeRecordReader.UT_UPSERT, 9);
    }
}
