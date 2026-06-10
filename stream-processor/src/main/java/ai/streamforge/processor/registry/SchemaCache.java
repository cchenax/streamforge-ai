package ai.streamforge.processor.registry;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Thread-safe LRU cache mapping Confluent schema IDs to parsed {@link Schema} objects.
 *
 * <p>Avro {@code Schema} parsing is expensive (JSON parse + validation).  This cache
 * ensures each schema ID is parsed at most once per Flink task-manager lifetime,
 * regardless of how many messages carry that ID.
 *
 * <p>The cache is bounded to {@code capacity} entries; the least-recently-used entry
 * is evicted when the bound is exceeded.
 */
public final class SchemaCache implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int capacity;

    // LinkedHashMap in access-order mode is a natural LRU structure.
    // Wrapped in synchronized for thread-safety (Flink task threads share state).
    private final Map<Integer, Schema> cache;

    public SchemaCache(int capacity) {
        this.capacity = Math.max(1, capacity);
        this.cache = new LinkedHashMap<Integer, Schema>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Schema> eldest) {
                return size() > SchemaCache.this.capacity;
            }
        };
    }

    /**
     * Return the cached {@link Schema} for {@code schemaId}, or {@code null} if absent.
     */
    public synchronized Schema get(int schemaId) {
        return cache.get(schemaId);
    }

    /**
     * Store {@code schema} under {@code schemaId}.
     * If the cache is full the least-recently-used entry is evicted.
     */
    public synchronized void put(int schemaId, Schema schema) {
        cache.put(schemaId, schema);
    }

    /** Return the current number of cached schemas. */
    public synchronized int size() {
        return cache.size();
    }

    /** Return {@code true} if the cache contains an entry for {@code schemaId}. */
    public synchronized boolean contains(int schemaId) {
        return cache.containsKey(schemaId);
    }

    /** Remove all entries (useful in tests). */
    public synchronized void clear() {
        cache.clear();
    }
}
