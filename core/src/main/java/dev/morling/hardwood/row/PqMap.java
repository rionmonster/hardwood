/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.row;

import java.util.List;

/**
 * Type-safe interface for accessing Parquet MAP values.
 * <p>
 * A MAP in Parquet is stored as a list of key-value entries. This interface
 * provides type-safe access to the entries with explicit key and value types.
 * </p>
 *
 * <pre>{@code
 * PqMap attributes = row.getValue(PqType.MAP, "attributes");
 * for (PqMap.Entry entry : attributes.getEntries()) {
 *     String key = entry.getKey(PqType.STRING);
 *     Integer value = entry.getValue(PqType.INT32);
 * }
 * }</pre>
 */
public interface PqMap {

    /**
     * Get all entries in this map.
     *
     * @return list of map entries
     */
    List<Entry> getEntries();

    /**
     * Get the number of entries in this map.
     *
     * @return the entry count
     */
    int size();

    /**
     * Check if this map is empty.
     *
     * @return true if the map has no entries
     */
    boolean isEmpty();

    /**
     * A single key-value entry in a map.
     */
    interface Entry {

        /**
         * Get the key value with type safety.
         *
         * @param type the expected type of the key
         * @param <K> the Java type of the key
         * @return the key value
         */
        <K> K getKey(PqType<K> type);

        /**
         * Get the value with type safety.
         *
         * @param type the expected type of the value
         * @param <V> the Java type of the value
         * @return the value, or null if the value is null
         */
        <V> V getValue(PqType<V> type);

        /**
         * Check if the value is null.
         *
         * @return true if the value is null
         */
        boolean isValueNull();
    }
}
