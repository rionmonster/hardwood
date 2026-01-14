/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import dev.morling.hardwood.schema.ColumnSchema;

/**
 * A batch of values with definition and repetition levels from a column.
 * Used internally by RowReader for batched parallel column fetching.
 *
 * <p>Values are stored with their rep/def levels for record assembly
 * by {@link RecordAssembler}.</p>
 */
public final class ColumnBatch {

    private final Object[] values;
    private final int[] definitionLevels;
    private final int[] repetitionLevels;
    private final int recordCount;
    private final ColumnSchema column;

    public ColumnBatch(Object[] values, int[] definitionLevels, int[] repetitionLevels,
                       int recordCount, ColumnSchema column) {
        this.values = values;
        this.definitionLevels = definitionLevels;
        this.repetitionLevels = repetitionLevels;
        this.recordCount = recordCount;
        this.column = column;
    }

    /**
     * Number of records in this batch.
     */
    public int size() {
        return recordCount;
    }

    /**
     * The column this batch belongs to.
     */
    public ColumnSchema getColumn() {
        return column;
    }

    /**
     * Number of values in this batch (may be greater than record count for repeated columns).
     */
    public int getValueCount() {
        return values.length;
    }

    /**
     * Get the value at the given index.
     */
    public Object getValue(int index) {
        return values[index];
    }

    /**
     * Get the definition level at the given index.
     */
    public int getDefinitionLevel(int index) {
        return definitionLevels[index];
    }

    /**
     * Get the repetition level at the given index.
     */
    public int getRepetitionLevel(int index) {
        return repetitionLevels[index];
    }

    /**
     * A value along with its definition and repetition levels.
     * Used for record assembly from column data.
     */
    public record ValueWithLevels(Object value, int defLevel, int repLevel) {
    }
}
