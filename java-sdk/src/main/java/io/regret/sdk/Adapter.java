package io.regret.sdk;

import java.util.List;

/**
 * User-facing adapter interface.
 */
public interface Adapter {

    /**
     * Execute a single operation against the target system.
     * Called concurrently for ops between fences.
     */
    OpResult executeOp(Operation op);

    /**
     * Read all records under the given key prefix.
     */
    List<Record> readState(String keyPrefix) throws Exception;

    /**
     * Cleanup data under the given key prefix.
     */
    default void cleanup(String keyPrefix) throws Exception {}
}
