/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.reader.RowReader;
import dev.morling.hardwood.row.Row;

import static org.assertj.core.api.Assertions.assertThat;

class RowReaderTest {

    @Test
    void testReadRowsFromPlainParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                // Verify we read 3 rows
                assertThat(rows).hasSize(3);

                // Verify first row: id=1, value=100
                Row row1 = rows.get(0);
                assertThat(row1.getColumnCount()).isEqualTo(2);
                assertThat(row1.getColumnName(0)).isEqualTo("id");
                assertThat(row1.getColumnName(1)).isEqualTo("value");
                assertThat(row1.getLong(0)).isEqualTo(1L);
                assertThat(row1.getLong("id")).isEqualTo(1L);
                assertThat(row1.getLong(1)).isEqualTo(100L);
                assertThat(row1.getLong("value")).isEqualTo(100L);
                assertThat(row1.isNull(0)).isFalse();
                assertThat(row1.isNull(1)).isFalse();

                // Verify second row: id=2, value=200
                Row row2 = rows.get(1);
                assertThat(row2.getLong(0)).isEqualTo(2L);
                assertThat(row2.getLong("id")).isEqualTo(2L);
                assertThat(row2.getLong(1)).isEqualTo(200L);
                assertThat(row2.getLong("value")).isEqualTo(200L);

                // Verify third row: id=3, value=300
                Row row3 = rows.get(2);
                assertThat(row3.getLong(0)).isEqualTo(3L);
                assertThat(row3.getLong("id")).isEqualTo(3L);
                assertThat(row3.getLong(1)).isEqualTo(300L);
                assertThat(row3.getLong("value")).isEqualTo(300L);
            }
        }
    }

    @Test
    void testReadRowsWithNulls() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed_with_nulls.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                // Verify we read 3 rows
                assertThat(rows).hasSize(3);

                // Verify first row: id=1, name="alice"
                Row row1 = rows.get(0);
                assertThat(row1.getColumnCount()).isEqualTo(2);
                assertThat(row1.getLong("id")).isEqualTo(1L);
                assertThat(row1.isNull("name")).isFalse();
                assertThat(row1.getString("name")).isEqualTo("alice");
                assertThat(row1.getByteArray("name")).isEqualTo("alice".getBytes());

                // Verify second row: id=2, name=null
                Row row2 = rows.get(1);
                assertThat(row2.getLong("id")).isEqualTo(2L);
                assertThat(row2.isNull("name")).isTrue();
                assertThat(row2.isNull(1)).isTrue();
                assertThat(row2.getString("name")).isNull();
                assertThat(row2.getByteArray("name")).isNull();

                // Verify third row: id=3, name="charlie"
                Row row3 = rows.get(2);
                assertThat(row3.getLong("id")).isEqualTo(3L);
                assertThat(row3.isNull("name")).isFalse();
                assertThat(row3.getString("name")).isEqualTo("charlie");
            }
        }
    }

    @Test
    void testReadRowsFromSnappyCompressedParquet() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_snappy.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                List<Row> rows = new ArrayList<>();
                for (Row row : rowReader) {
                    rows.add(row);
                }

                // Verify we read 3 rows
                assertThat(rows).hasSize(3);

                // Verify the rows have correct values
                assertThat(rows.get(0).getLong("id")).isEqualTo(1L);
                assertThat(rows.get(0).getLong("value")).isEqualTo(100L);

                assertThat(rows.get(1).getLong("id")).isEqualTo(2L);
                assertThat(rows.get(1).getLong("value")).isEqualTo(200L);

                assertThat(rows.get(2).getLong("id")).isEqualTo(3L);
                assertThat(rows.get(2).getLong("value")).isEqualTo(300L);
            }
        }
    }

    @Test
    void testIteratorMultipleTimes() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            // First iteration
            try (RowReader rowReader1 = fileReader.createRowReader()) {
                int count1 = 0;
                for (Row row : rowReader1) {
                    count1++;
                }
                assertThat(count1).isEqualTo(3);
            }

            // Second iteration (new reader)
            try (RowReader rowReader2 = fileReader.createRowReader()) {
                int count2 = 0;
                for (Row row : rowReader2) {
                    count2++;
                }
                assertThat(count2).isEqualTo(3);
            }
        }
    }

    @Test
    void testRowReaderWithForEach() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                int rowCount = 0;
                long sumIds = 0;
                long sumValues = 0;

                // Use enhanced for loop
                for (Row row : rowReader) {
                    rowCount++;
                    sumIds += row.getLong("id");
                    sumValues += row.getLong("value");
                }

                assertThat(rowCount).isEqualTo(3);
                assertThat(sumIds).isEqualTo(6L); // 1 + 2 + 3
                assertThat(sumValues).isEqualTo(600L); // 100 + 200 + 300
            }
        }
    }

    @Test
    void testColumnAccessByNameAndIndex() throws Exception {
        Path parquetFile = Paths.get("src/test/resources/plain_uncompressed.parquet");

        try (ParquetFileReader fileReader = ParquetFileReader.open(parquetFile)) {
            try (RowReader rowReader = fileReader.createRowReader()) {
                Row firstRow = rowReader.iterator().next();

                // Access by index
                long idByIndex = firstRow.getLong(0);
                long valueByIndex = firstRow.getLong(1);

                // Access by name
                long idByName = firstRow.getLong("id");
                long valueByName = firstRow.getLong("value");

                // Should be the same
                assertThat(idByIndex).isEqualTo(idByName).isEqualTo(1L);
                assertThat(valueByIndex).isEqualTo(valueByName).isEqualTo(100L);
            }
        }
    }
}
