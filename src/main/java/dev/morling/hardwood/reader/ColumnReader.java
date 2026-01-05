/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.internal.reader.ColumnBatch;
import dev.morling.hardwood.internal.reader.PageReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Reader for a column chunk.
 */
public class ColumnReader {

    private final ColumnSchema column;
    private final ColumnMetaData columnMetaData;
    private final PageReader pageReader;
    private final int maxDefinitionLevel;
    private final long totalValues;

    // State for incremental reading
    private PageReader.Page currentPage;
    private int currentPagePosition;
    private long valuesRead;

    public ColumnReader(RandomAccessFile file, ColumnSchema column, ColumnChunk columnChunk) {
        this.column = column;
        this.columnMetaData = columnChunk.metaData();
        this.pageReader = new PageReader(file, columnMetaData, column);
        this.maxDefinitionLevel = column.getMaxDefinitionLevel();
        this.totalValues = columnMetaData.numValues();
        this.currentPage = null;
        this.currentPagePosition = 0;
        this.valuesRead = 0;
    }

    /**
     * Read all values from this column chunk.
     * Null values are represented as null in the returned list.
     */
    public List<Object> readAll() throws IOException {
        List<Object> allValues = new ArrayList<>();

        while (hasNext()) {
            allValues.add(readNext());
        }

        return allValues;
    }

    /**
     * Check if there are more values to read from this column.
     */
    public boolean hasNext() {
        return valuesRead < totalValues;
    }

    /**
     * Read the next value from this column.
     * Returns null for optional fields when the value is not present.
     */
    public Object readNext() throws IOException {
        if (!hasNext()) {
            throw new IllegalStateException("No more values to read");
        }

        // Load next page if needed
        if (currentPage == null || currentPagePosition >= currentPage.numValues()) {
            currentPage = pageReader.readPage();
            currentPagePosition = 0;
            if (currentPage == null) {
                throw new IOException("Unexpected end of column data");
            }
        }

        Object value;
        if (maxDefinitionLevel == 0) {
            // Required field - all values present
            value = currentPage.values()[currentPagePosition];
        }
        else {
            // Optional field - check definition level
            if (currentPage.definitionLevels()[currentPagePosition] == maxDefinitionLevel) {
                // Value is present
                value = currentPage.values()[currentPagePosition];
            }
            else {
                // Value is null
                value = null;
            }
        }

        currentPagePosition++;
        valuesRead++;
        return value;
    }

    /**
     * Read a batch of values from this column.
     * Returns up to batchSize values, or fewer if end of column is reached.
     */
    ColumnBatch readBatch(int batchSize) throws IOException {
        Object[] values = new Object[batchSize];
        int count = 0;

        while (count < batchSize && hasNext()) {
            values[count++] = readNext();
        }

        return new ColumnBatch(values, count, column);
    }

    public ColumnSchema getColumnSchema() {
        return column;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }
}
