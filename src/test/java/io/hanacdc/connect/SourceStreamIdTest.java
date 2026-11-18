package io.hanacdc.connect;

import io.hanacdc.control.PersistenceService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SourceStreamIdTest {
    @Test
    void roundTripsStableIdentityWithoutHostCoordinates() {
        SourceStreamId stream = SourceStreamId.from(
                new PersistenceService("hana02", 39040, "INDEXSERVER", 3));

        assertEquals("volume=3;service=indexserver", stream.canonical());
        assertEquals(stream, SourceStreamId.parse(stream.canonical()));
    }

    @Test
    void rejectsMalformedIdentity() {
        assertThrows(IllegalArgumentException.class,
                () -> SourceStreamId.parse("host=hana02;volume=3"));
        assertThrows(IllegalArgumentException.class,
                () -> new SourceStreamId(0, "indexserver"));
    }
}
