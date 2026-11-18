package io.hanacdc.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class KafkaTopicAuditMainTest {
    @Test
    void validatesOrderingOperationsAndFinalKeys() throws Exception {
        var audit = new KafkaTopicAuditMain.Audit();
        audit.accept("ID=1", "c", 10, 1);
        audit.accept("ID=1", "u", 20, 2);
        audit.accept("ID=2", "c", 30, 3);
        audit.accept("ID=1", "d", 40, 4);

        var result = audit.require(4, 1,
                new KafkaTopicAuditMain.ExpectedOperations(2, 1, 1));
        assertEquals(4, result.uniqueEvents());
        assertEquals(1, result.liveKeys());
        assertEquals(10, result.firstPosition());
        assertEquals(40, result.lastPosition());
    }

    @Test
    void rejectsDuplicateOrReorderedFreshTopic() throws Exception {
        var duplicate = new KafkaTopicAuditMain.Audit();
        duplicate.accept("ID=1", "c", 10, 1);
        duplicate.accept("ID=1", "c", 10, 1);
        assertThrows(IllegalStateException.class,
                () -> duplicate.require(1, 1, null));

        var reordered = new KafkaTopicAuditMain.Audit();
        reordered.accept("ID=1", "c", 20, 1);
        assertThrows(IllegalStateException.class,
                () -> reordered.accept("ID=2", "c", 10, 2));
    }

    @Test
    void distinguishesChangesOfSameKeyInsideOneTransaction() throws Exception {
        var audit = new KafkaTopicAuditMain.Audit();
        audit.accept("ID=1", "c", 10, 1, 1);
        audit.accept("ID=1", "u", 10, 1, 2);

        var result = audit.require(2, 1,
                new KafkaTopicAuditMain.ExpectedOperations(1, 1, 0));
        assertEquals(2, result.uniqueEvents());
    }

    @Test
    void acceptsOnlyIdenticalReplayAndKeepsUniqueOrdering() throws Exception {
        var audit = new KafkaTopicAuditMain.Audit();
        audit.accept("ID=1", "c", 10, 1);
        audit.accept("ID=1", "u", 20, 2);
        audit.accept("ID=1", "c", 10, 1);
        audit.accept("ID=1", "u", 20, 2);
        audit.accept("ID=2", "c", 30, 3);

        var result = audit.require(3, 2,
                new KafkaTopicAuditMain.ExpectedOperations(2, 1, 0), 2);
        assertEquals(5, result.records());
        assertEquals(2, result.duplicates());
        assertEquals(30, result.lastPosition());

        var changed = new KafkaTopicAuditMain.Audit();
        changed.accept("ID=1", "c", 10, 1);
        assertThrows(IllegalStateException.class,
                () -> changed.accept("ID=1", "d", 10, 1));
    }
}
