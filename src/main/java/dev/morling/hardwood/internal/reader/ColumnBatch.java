/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Holds a batch of values read from a single column.
 * Used internally by RowReader for batched parallel column fetching.
 */
public class ColumnBatch {

    private final Object[] values;
    private final int size;
    private final ColumnSchema column;

    public ColumnBatch(Object[] values, int size, ColumnSchema column) {
        this.values = values;
        this.size = size;
        this.column = column;
    }

    /**
     * Get value at the given index within this batch.
     */
    public Object get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for batch size " + size);
        }
        return values[index];
    }

    /**
     * Number of values in this batch.
     */
    public int size() {
        return size;
    }

    /**
     * The column this batch belongs to.
     */
    public ColumnSchema getColumn() {
        return column;
    }
}
