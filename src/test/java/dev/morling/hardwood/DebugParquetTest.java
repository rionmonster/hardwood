/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

public class DebugParquetTest {

    public static void main(String[] args) throws Exception {
        String file = "src/test/resources/yellow_tripdata_2025-01.parquet";

        System.out.println("=== " + file + " ===");
        try (ParquetFileReader reader = ParquetFileReader.open(Paths.get(file))) {
            System.out.println("Version: " + reader.getFileMetaData().version());
            System.out.println("Num rows: " + reader.getFileMetaData().numRows());
            System.out.println("Row groups: " + reader.getFileMetaData().rowGroups().size());
            System.out.println();

            FileSchema schema = reader.getFileSchema();
            int colCount = schema.getColumnCount();

            // Calculate column widths
            int[] widths = new int[colCount];
            for (int i = 0; i < colCount; i++) {
                widths[i] = Math.max(schema.getColumn(i).name().length(), 8);
            }

            // Print header
            StringBuilder header = new StringBuilder("| ");
            StringBuilder separator = new StringBuilder("+-");
            for (int i = 0; i < colCount; i++) {
                header.append(padRight(schema.getColumn(i).name(), widths[i])).append(" | ");
                separator.append("-".repeat(widths[i])).append("-+-");
            }
            System.out.println(separator);
            System.out.println(header);
            System.out.println(separator);

            // Print rows
            try (RowReader rowReader = reader.createRowReader()) {
                int rowNum = 0;
                for (PqRow row : rowReader) {
                    StringBuilder line = new StringBuilder("| ");
                    for (int i = 0; i < colCount; i++) {
                        String value = formatValue(row, i, schema.getColumn(i));
                        // Adjust width if value is longer
                        if (value.length() > widths[i]) {
                            value = value.substring(0, widths[i] - 2) + "..";
                        }
                        line.append(padRight(value, widths[i])).append(" | ");
                    }
                    System.out.println(line);
                    if (++rowNum >= 5) {
                        break;
                    }
                }
            }
            System.out.println(separator);
        }
    }

    private static String formatValue(PqRow row, int col, ColumnSchema schema) {
        if (row.isNull(col)) {
            return "null";
        }

        Object value = getValue(row, col, schema);

        if (value instanceof Instant instant) {
            // Format timestamp as local datetime for readability
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
                    .toString().replace("T", " ");
        }
        if (value instanceof Double d) {
            return String.format("%.2f", d);
        }
        return String.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private static Object getValue(PqRow row, int col, ColumnSchema schema) {
        // Use logical type if available
        LogicalType logicalType = schema.logicalType();
        if (logicalType != null) {
            PqType<?> pqType = logicalType.toPqType();
            if (pqType != null) {
                return row.getValue(pqType, col);
            }
        }

        // Fall back to physical type
        PhysicalType physicalType = schema.type();
        return switch (physicalType) {
            case BOOLEAN -> row.getValue(PqType.BOOLEAN, col);
            case INT32 -> row.getValue(PqType.INT32, col);
            case INT64 -> row.getValue(PqType.INT64, col);
            case FLOAT -> row.getValue(PqType.FLOAT, col);
            case DOUBLE -> row.getValue(PqType.DOUBLE, col);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.getValue(PqType.STRING, col);
            default -> row.getValue(PqType.BINARY, col);
        };
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
    }
}
