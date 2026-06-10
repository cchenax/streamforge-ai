package ai.streamforge.processor.registry;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaCacheTest {

    private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
    private static final Schema INT_SCHEMA    = Schema.create(Schema.Type.INT);
    private static final Schema LONG_SCHEMA   = Schema.create(Schema.Type.LONG);

    @Test
    void get_returns_null_for_unknown_id() {
        SchemaCache cache = new SchemaCache(10);
        assertNull(cache.get(99));
    }

    @Test
    void put_and_get_returns_schema() {
        SchemaCache cache = new SchemaCache(10);
        cache.put(1, STRING_SCHEMA);
        assertSame(STRING_SCHEMA, cache.get(1));
    }

    @Test
    void contains_reflects_presence() {
        SchemaCache cache = new SchemaCache(10);
        assertFalse(cache.contains(5));
        cache.put(5, INT_SCHEMA);
        assertTrue(cache.contains(5));
    }

    @Test
    void lru_eviction_on_capacity_exceeded() {
        // Capacity = 2 — inserting a 3rd entry should evict the LRU (id=1)
        SchemaCache cache = new SchemaCache(2);
        cache.put(1, STRING_SCHEMA);
        cache.put(2, INT_SCHEMA);
        cache.get(1);              // access id=1 → makes id=2 the LRU
        cache.put(3, LONG_SCHEMA); // evicts id=2
        assertNull(cache.get(2), "id=2 should have been evicted");
        assertNotNull(cache.get(1));
        assertNotNull(cache.get(3));
        assertEquals(2, cache.size());
    }

    @Test
    void overwrite_same_id_does_not_grow() {
        SchemaCache cache = new SchemaCache(5);
        cache.put(1, STRING_SCHEMA);
        cache.put(1, INT_SCHEMA);
        assertEquals(1, cache.size());
        assertSame(INT_SCHEMA, cache.get(1));
    }

    @Test
    void clear_empties_cache() {
        SchemaCache cache = new SchemaCache(10);
        cache.put(1, STRING_SCHEMA);
        cache.put(2, INT_SCHEMA);
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void capacity_of_one_always_keeps_latest() {
        SchemaCache cache = new SchemaCache(1);
        cache.put(1, STRING_SCHEMA);
        cache.put(2, INT_SCHEMA);
        assertNull(cache.get(1));
        assertSame(INT_SCHEMA, cache.get(2));
    }
}
