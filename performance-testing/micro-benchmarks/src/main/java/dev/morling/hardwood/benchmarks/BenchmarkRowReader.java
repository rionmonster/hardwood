/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.benchmarks;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * A minimal row reader for benchmarking purposes.
 * <p>
 * Unlike FlatRowReader, this class operates in "manual mode" where column data
 * is explicitly loaded via {@link #loadBatch(TypedColumnData[])} rather than
 * read from a file. This allows benchmarking the row access layer in isolation
 * without file I/O overhead.
 * </p>
 */
final class BenchmarkRowReader {

    private final FileSchema schema;

    private TypedColumnData[] columnData;
    private BitSet[] nulls;
    private int recordCount = 0;

    private int rowIndex = -1;

    BenchmarkRowReader(FileSchema schema) {
        this.schema = schema;
    }

    /**
     * Load a batch of column data for row-oriented access.
     */
    void loadBatch(TypedColumnData[] newColumnData) {
        this.columnData = newColumnData;
        this.rowIndex = -1;
        this.recordCount = newColumnData.length > 0 ? newColumnData[0].recordCount() : 0;

        this.nulls = new BitSet[newColumnData.length];
        for (int i = 0; i < newColumnData.length; i++) {
			nulls[i] = newColumnData[i].nulls();
		}
    }

    boolean hasNext() {
        return rowIndex + 1 < recordCount;
    }

    void next() {
        rowIndex++;
    }

    // ==================== Primitive Type Accessors ====================

    int getInt(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((TypedColumnData.IntColumn) data).get(rowIndex);
    }

    long getLong(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((TypedColumnData.LongColumn) data).get(rowIndex);
    }

    float getFloat(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((TypedColumnData.FloatColumn) data).get(rowIndex);
    }

    double getDouble(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((TypedColumnData.DoubleColumn) data).get(rowIndex);
    }

    boolean getBoolean(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            throw new NullPointerException("Column " + columnIndex + " is null");
        }
        return ((TypedColumnData.BooleanColumn) data).get(rowIndex);
    }

    // ==================== Object Type Accessors ====================

    String getString(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        return new String(((TypedColumnData.ByteArrayColumn) data).get(rowIndex), StandardCharsets.UTF_8);
    }

    byte[] getBinary(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        return ((TypedColumnData.ByteArrayColumn) data).get(rowIndex);
    }

    LocalDate getDate(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        int rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToDate(rawValue, col.type());
    }

    LocalTime getTime(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        Object rawValue;
        if (col.type() == dev.morling.hardwood.metadata.PhysicalType.INT32) {
            rawValue = ((TypedColumnData.IntColumn) data).get(rowIndex);
        }
        else {
            rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        }
        return LogicalTypeConverter.convertToTime(rawValue, col.type(), (LogicalType.TimeType) col.logicalType());
    }

    Instant getTimestamp(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        long rawValue = ((TypedColumnData.LongColumn) data).get(rowIndex);
        return LogicalTypeConverter.convertToTimestamp(rawValue, col.type(), (LogicalType.TimestampType) col.logicalType());
    }

    BigDecimal getDecimal(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        Object rawValue = switch (col.type()) {
            case INT32 -> ((TypedColumnData.IntColumn) data).get(rowIndex);
            case INT64 -> ((TypedColumnData.LongColumn) data).get(rowIndex);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ((TypedColumnData.ByteArrayColumn) data).get(rowIndex);
            default -> throw new IllegalArgumentException("Unexpected physical type for DECIMAL: " + col.type());
        };
        return LogicalTypeConverter.convertToDecimal(rawValue, col.type(), (LogicalType.DecimalType) col.logicalType());
    }

    UUID getUuid(int columnIndex) {
        TypedColumnData data = columnData[columnIndex];
        if (isNull(columnIndex)) {
            return null;
        }
        ColumnSchema col = schema.getColumn(columnIndex);
        return LogicalTypeConverter.convertToUuid(((TypedColumnData.ByteArrayColumn) data).get(rowIndex), col.type());
    }

    // ==================== Metadata ====================

    boolean isNull(int columnIndex) {
        BitSet columnNulls = nulls[columnIndex];
        return columnNulls != null && columnNulls.get(rowIndex) == true;
    }
}
