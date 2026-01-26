/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import dev.morling.hardwood.internal.reader.BatchRowView;
import dev.morling.hardwood.internal.reader.ColumnValueIterator;
import dev.morling.hardwood.internal.reader.MutableStruct;
import dev.morling.hardwood.internal.reader.PageCursor;
import dev.morling.hardwood.internal.reader.PageInfo;
import dev.morling.hardwood.internal.reader.PageScanner;
import dev.morling.hardwood.internal.reader.PqRowImpl;
import dev.morling.hardwood.internal.reader.RecordAssembler;
import dev.morling.hardwood.internal.reader.TypedColumnData;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.row.PqRow;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Provides row-oriented iteration over a Parquet file.
 * Uses on-demand page loading with async decoding for efficient memory usage.
 * Pages are fetched when needed using CompletableFuture for parallel loading across columns.
 */
public class RowReader implements Iterable<PqRow>, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(RowReader.class.getName());

    private final FileSchema schema;
    private final FileChannel channel;
    private final List<RowGroup> rowGroups;
    private final boolean flatSchema;
    private final ExecutorService executor;
    private final String fileName;

    private PageScanner scanner;  // Keep reference to prevent GC of mapped buffers
    private volatile boolean closed;

    public RowReader(FileSchema schema, FileChannel channel, List<RowGroup> rowGroups,
                     ExecutorService executor, String fileName) {
        this.schema = schema;
        this.channel = channel;
        this.rowGroups = rowGroups;
        this.flatSchema = schema.isFlatSchema();
        this.executor = executor;
        this.fileName = fileName;
    }

    @Override
    public Iterator<PqRow> iterator() {
        return flatSchema ? new FlatSchemaIterator() : new NestedSchemaIterator();
    }

    @Override
    public void close() {
        closed = true;
        // Executor is owned by ParquetFileReader, not shut down here
    }

    /**
     * Optimized iterator for flat schemas (no nesting).
     * Uses ColumnValueIterator for batch prefetching into aligned TypedColumnData vectors.
     * Each prefetch aligns data across columns by record count, handling different page sizes.
     */
    private class FlatSchemaIterator implements Iterator<PqRow> {

        private static final int BATCH_SIZE = 8192;

        private final ColumnValueIterator[] iterators;
        private TypedColumnData[] columnData;
        private int rowIndex = 0;
        private int batchSize = 0;
        private boolean exhausted = false;

        FlatSchemaIterator() {
            try {
                LOG.log(System.Logger.Level.DEBUG, "Starting to parse file ''{0}'' with {1} row groups, {2} columns",
                        fileName, rowGroups.size(), schema.getColumnCount());

                RowReader.this.scanner = new PageScanner(channel, schema, rowGroups);
                List<List<PageInfo>> pageInfosByColumn = RowReader.this.scanner.scanPages();

                int columnCount = schema.getColumnCount();
                iterators = new ColumnValueIterator[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    PageCursor pageCursor = new PageCursor(pageInfosByColumn.get(i), executor);
                    iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(i), executor);
                }

                // Eagerly load first batch
                loadNextBatch();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to initialize page scanner", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (closed || exhausted) {
                return false;
            }
            // If we have more rows in current batch
            if (rowIndex < batchSize) {
                return true;
            }
            // Try to load next batch
            return loadNextBatch();
        }

        @Override
        public PqRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available");
            }
            return new BatchRowView(columnData, schema, rowIndex++);
        }

        @SuppressWarnings("unchecked")
        private boolean loadNextBatch() {
            // Prefetch all columns in parallel
            CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                final int col = i;
                futures[i] = CompletableFuture.supplyAsync(() -> iterators[col].prefetch(BATCH_SIZE), executor);
            }

            // Wait for all columns and collect results
            CompletableFuture.allOf(futures).join();

            TypedColumnData[] newColumnData = new TypedColumnData[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                newColumnData[i] = futures[i].join();
                if (newColumnData[i].recordCount() == 0) {
                    exhausted = true;
                    return false;
                }
            }
            columnData = newColumnData;

            // All columns should have the same record count due to alignment
            batchSize = columnData[0].recordCount();
            rowIndex = 0;
            return batchSize > 0;
        }
    }

    /**
     * Iterator for nested schemas (lists, maps, structs).
     * Uses ColumnValueIterator for batch prefetching and RecordAssembler
     * to handle repetition/definition levels.
     */
    private class NestedSchemaIterator implements Iterator<PqRow> {

        private static final int BATCH_SIZE = 8192;

        private final RecordAssembler assembler = new RecordAssembler(schema);
        private ColumnValueIterator[] iterators;
        private List<TypedColumnData> columnData;
        private int rowIndex = 0;
        private int batchSize = 0;
        private boolean exhausted = false;

        NestedSchemaIterator() {
            try {
                LOG.log(System.Logger.Level.DEBUG, "Starting to parse file ''{0}'' with {1} row groups, {2} columns",
                        fileName, rowGroups.size(), schema.getColumnCount());

                RowReader.this.scanner = new PageScanner(channel, schema, rowGroups);
                List<List<PageInfo>> pageInfosByColumn = RowReader.this.scanner.scanPages();

                int columnCount = schema.getColumnCount();
                iterators = new ColumnValueIterator[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    PageCursor pageCursor = new PageCursor(pageInfosByColumn.get(i), executor);
                    iterators[i] = new ColumnValueIterator(pageCursor, schema.getColumn(i), executor);
                }

                // Eagerly load first batch
                loadNextBatch();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to initialize page scanner", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (closed || exhausted) {
                return false;
            }
            if (rowIndex < batchSize) {
                return true;
            }
            return loadNextBatch();
        }

        @Override
        public PqRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more rows available");
            }

            MutableStruct rowValues = assembler.assembleRow(columnData, rowIndex++);
            return new PqRowImpl(rowValues, schema);
        }

        @SuppressWarnings("unchecked")
        private boolean loadNextBatch() {
            // Prefetch all columns in parallel
            CompletableFuture<TypedColumnData>[] futures = new CompletableFuture[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                final int col = i;
                futures[i] = CompletableFuture.supplyAsync(() -> iterators[col].prefetch(BATCH_SIZE), executor);
            }

            // Wait for all columns and collect results
            CompletableFuture.allOf(futures).join();

            TypedColumnData[] newColumnData = new TypedColumnData[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                newColumnData[i] = futures[i].join();
                if (newColumnData[i].recordCount() == 0) {
                    exhausted = true;
                    return false;
                }
            }
            columnData = List.of(newColumnData);

            // All columns should have the same record count due to alignment
            batchSize = newColumnData[0].recordCount();
            rowIndex = 0;
            return batchSize > 0;
        }
    }
}
