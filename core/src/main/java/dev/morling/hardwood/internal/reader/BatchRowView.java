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
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
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
 * Immutable view into batch column data at a specific row index.
 * <p>
 * This class provides PqRow access to data stored in {@link TypedColumnData} arrays.
 * Each instance captures a snapshot of the column data references and row index,
 * remaining valid even after the iterator loads subsequent batches.
 * </p>
 */
public class BatchRowView implements PqRow {

    private final TypedColumnData[] columnData;
    private final FileSchema schema;
    private final int rowIndex;

    /**
     * Create a BatchRowView for a specific row in the batch.
     *
     * @param columnData the column data for this batch (should be a defensive copy if batch may change)
     * @param schema     the file schema
     * @param rowIndex   the row index within the batch (immutable)
     */
    public BatchRowView(TypedColumnData[] columnData, FileSchema schema, int rowIndex) {
        this.columnData = columnData;
        this.schema = schema;
        this.rowIndex = rowIndex;
    }

    // ==================== Primitive Types ====================

    @Override
    public int getInt(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT32);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.IntColumn) data).get(rowIndex);
    }

    @Override
    public long getLong(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.INT64);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.LongColumn) data).get(rowIndex);
    }

    @Override
    public float getFloat(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.FLOAT);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.FloatColumn) data).get(rowIndex);
    }

    @Override
    public double getDouble(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.DOUBLE);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.DoubleColumn) data).get(rowIndex);
    }

    @Override
    public boolean getBoolean(String name) {
        ColumnSchema col = schema.getColumn(name);
        validatePhysicalType(col, PhysicalType.BOOLEAN);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            throw new NullPointerException("Field '" + name + "' is null");
        }
        return ((TypedColumnData.BooleanColumn) data).get(rowIndex);
    }

    // ==================== Object Types ====================

    @Override
    public String getString(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        return new String(((TypedColumnData.ByteArrayColumn) data).get(rowIndex), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        return ((TypedColumnData.ByteArrayColumn) data).get(rowIndex);
    }

    @Override
    public LocalDate getDate(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        int rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    @Override
    public LocalTime getTime(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        Object rawValue;
        if (col.type() == PhysicalType.INT32) {
            rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        }
        else {
            rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    @Override
    public Instant getTimestamp(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        long rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    @Override
    public BigDecimal getDecimal(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        Object rawValue = switch (col.type()) {
            case INT32 -> ((TypedColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((TypedColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((TypedColumnData.ByteArrayColumn) data).get(rowIndex);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(), (LogicalType.DecimalType) col.logicalType());
    }

    @Override
    public UUID getUuid(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        return LogicalTypeConverter.convertToUuid(((TypedColumnData.ByteArrayColumn) data).get(rowIndex), col.type());
    }

    // ==================== Nested Types (not supported for flat schemas) ====================

    @Override
    public PqRow getRow(String name) {
        throw new UnsupportedOperationException(
                "Nested struct access not supported in batch mode. Schema is expected to be flat.");
    }

    @Override
    public PqIntList getListOfInts(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in batch mode. Schema is expected to be flat.");
    }

    @Override
    public PqLongList getListOfLongs(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in batch mode. Schema is expected to be flat.");
    }

    @Override
    public PqDoubleList getListOfDoubles(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in batch mode. Schema is expected to be flat.");
    }

    @Override
    public PqList getList(String name) {
        throw new UnsupportedOperationException(
                "List access not supported in batch mode. Schema is expected to be flat.");
    }

    @Override
    public PqMap getMap(String name) {
        throw new UnsupportedOperationException(
                "Map access not supported in batch mode. Schema is expected to be flat.");
    }

    // ==================== Generic Fallback ====================

    @Override
    public Object getValue(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        TypedColumnData data = columnData[idx];
        if (data.isNull(rowIndex)) {
            return null;
        }
        return data.getValue(rowIndex);
    }

    // ==================== Metadata ====================

    @Override
    public boolean isNull(String name) {
        ColumnSchema col = schema.getColumn(name);
        int idx = col.columnIndex();
        return columnData[idx].isNull(rowIndex);
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

    private void validatePhysicalType(ColumnSchema col, PhysicalType... expectedTypes) {
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
