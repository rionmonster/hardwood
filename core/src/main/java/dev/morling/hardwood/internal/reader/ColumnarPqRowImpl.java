/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqDoubleList;
import dev.morling.hardwood.row.PqIntList;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqLongList;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Implementation of PqRow that reads directly from column batches at a given row index.
 * <p>
 * This implementation is optimized for flat schemas (no nested/repeated fields) where
 * row N maps directly to index N in each column batch. For flat schemas, this avoids
 * the overhead of record assembly via {@link RecordAssembler}.
 * </p>
 * <p>
 * Nested field accessors ({@link #getRow}, {@link #getList}, {@link #getMap}) throw
 * {@link UnsupportedOperationException} as this implementation is only used for flat schemas.
 * </p>
 */
public class ColumnarPqRowImpl implements PqRow {

    private final List<ColumnBatch> batches;
    private final int rowIndex;
    private final FileSchema schema;

    /**
     * Create a columnar PqRow backed by column batches.
     *
     * @param batches  the column batches (one per column)
     * @param rowIndex the row index within the current batch
     * @param schema   the file schema for column lookup
     */
    public ColumnarPqRowImpl(List<ColumnBatch> batches, int rowIndex, FileSchema schema) {
        this.batches = batches;
        this.rowIndex = rowIndex;
        this.schema = schema;
    }

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.INT32);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        if (batch.hasTypedData()) {
            return batch.getIntValue(rowIndex);
        }
        return (Integer) batch.getValues()[rowIndex];
    }

    @Override
    public long getLong(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.INT64);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        if (batch.hasTypedData()) {
            return batch.getLongValue(rowIndex);
        }
        return (Long) batch.getValues()[rowIndex];
    }

    @Override
    public float getFloat(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.FLOAT);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return (Float) getObjectValue(batch);
    }

    @Override
    public double getDouble(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.DOUBLE);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        if (batch.hasTypedData()) {
            return batch.getDoubleValue(rowIndex);
        }
        return (Double) batch.getValues()[rowIndex];
    }

    @Override
    public boolean getBoolean(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.BOOLEAN);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return (Boolean) getObjectValue(batch);
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.BYTE_ARRAY);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        if (rawValue instanceof String s) {
            return s;
        }
        return new String((byte[]) rawValue, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        ColumnSchema col = schema.getColumn(name);
        validateType(col, PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        return (byte[]) getObjectValue(batch);
    }

    @Override
    public LocalDate getDate(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        return (LocalDate) LogicalTypeConverter.convert(rawValue, col.type(), col.logicalType());
    }

    @Override
    public LocalTime getTime(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        return (LocalTime) LogicalTypeConverter.convert(rawValue, col.type(), col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        return (Instant) LogicalTypeConverter.convert(rawValue, col.type(), col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        return (BigDecimal) LogicalTypeConverter.convert(rawValue, col.type(), col.logicalType());
    }

    @Override
    public UUID getUuid(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        Object rawValue = getObjectValue(batch);
        return (UUID) LogicalTypeConverter.convert(rawValue, col.type(), col.logicalType());
    }

    // ==================== Nested Types (not supported for flat schemas) ====================

    @Override
    public PqRow getRow(String name) {
        throw new UnsupportedOperationException(
                "Nested struct access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqIntList getListOfInts(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqList getList(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in columnar mode. Schema is expected to be flat.");
    }

    @Override
    public PqMap getMap(String name) {
        throw new UnsupportedOperationException(
                "Map access not supported in columnar mode. Schema is expected to be flat.");
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        if (isNullAt(batch, col)) {
            return null;
        }
        return getObjectValue(batch);
    }

    /**
     * Get an object value from the batch, handling both typed and untyped cases.
     */
    private Object getObjectValue(ColumnBatch batch) {
        if (batch.hasTypedData()) {
            TypedColumnData data = batch.getTypedData();
            if (data instanceof TypedColumnData.ObjectColumn objCol) {
                return objCol.get(rowIndex);
            }
            else if (data instanceof TypedColumnData.LongColumn longCol) {
                return longCol.get(rowIndex);
            }
            else if (data instanceof TypedColumnData.DoubleColumn doubleCol) {
                return doubleCol.get(rowIndex);
            }
            else if (data instanceof TypedColumnData.IntColumn intCol) {
                return intCol.get(rowIndex);
            }
        }
        return batch.getValues()[rowIndex];
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        ColumnSchema col = schema.getColumn(name);
        ColumnBatch batch = batches.get(col.columnIndex());
        return isNullAt(batch, col);
    }

    /**
     * Check if the value at the current row index is null.
     * A value is null if its definition level is less than the max definition level.
     */
    private boolean isNullAt(ColumnBatch batch, ColumnSchema col) {
        int maxDefLevel = col.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            return false; // Required field - never null
        }
        return batch.getDefinitionLevels()[rowIndex] < maxDefLevel;
    }

    @Override
    public int getFieldCount() {
        return schema.getColumnCount();
    }

    @Override
    public String getFieldName(int index) {
        return schema.getColumn(index).name();
    }

    // ==================== Internal Helpers ====================

    /**
     * Validate that the column has the expected physical type.
     */
    private void validateType(ColumnSchema col, PhysicalType... expectedTypes) {
        for (PhysicalType expected : expectedTypes) {
            if (col.type() == expected) {
                return;
            }
        }
        String expectedStr = expectedTypes.length == 1
                ? expectedTypes[0].toString()
                : java.util.Arrays.toString(expectedTypes);
        throw new IllegalArgumentException(
                "Field '" + col.name() + "' has physical type " + col.type() + ", expected " + expectedStr);
    }
}
