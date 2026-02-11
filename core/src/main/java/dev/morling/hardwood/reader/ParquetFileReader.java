/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.reader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.morling.hardwood.internal.reader.ParquetMetadataReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.FileMetaData;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;
import dev.morling.hardwood.schema.ProjectedSchema;

/**
 * Reader for individual Parquet files.
 *
 * <p>For single-file usage:</p>
 * <pre>{@code
 * try (ParquetFileReader reader = ParquetFileReader.open(path)) {
 *     RowReader rows = reader.createRowReader();
 *     // ...
 * }
 * }</pre>
 *
 * <p>For multi-file usage with shared thread pool, use {@link Hardwood}.</p>
 */
public class ParquetFileReader implements AutoCloseable {

    private final Path path;
    private final FileChannel channel;
    private final FileMetaData fileMetaData;
    private final HardwoodContext context;
    private final boolean ownsContext;

    private ParquetFileReader(Path path, FileChannel channel, FileMetaData fileMetaData,
                              HardwoodContext context, boolean ownsContext) {
        this.path = path;
        this.channel = channel;
        this.fileMetaData = fileMetaData;
        this.context = context;
        this.ownsContext = ownsContext;
    }

    /**
     * Open a Parquet file with a dedicated context.
     * The context is closed when this reader is closed.
     */
    public static ParquetFileReader open(Path path) throws IOException {
        HardwoodContext context = HardwoodContext.create();
        return open(path, context, true);
    }

    /**
     * Open a Parquet file with a shared context.
     * The context is NOT closed when this reader is closed.
     */
    static ParquetFileReader open(Path path, HardwoodContext context) throws IOException {
        return open(path, context, false);
    }

    private static ParquetFileReader open(Path path, HardwoodContext context,
                                          boolean ownsContext) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            FileMetaData fileMetaData = ParquetMetadataReader.readMetadata(channel, path);
            return new ParquetFileReader(path, channel, fileMetaData, context, ownsContext);
        }
        catch (Exception e) {
            // Close channel if there was an error during initialization
            try {
                channel.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    public FileMetaData getFileMetaData() {
        return fileMetaData;
    }

    public FileSchema getFileSchema() {
        return FileSchema.fromSchemaElements(fileMetaData.schema());
    }

    public ColumnReader getColumnReader(ColumnSchema idColumn, ColumnChunk idColumnChunk) throws IOException {
        return new ColumnReader(path, idColumn, idColumnChunk, context);
    }

    /**
     * Create a RowReader that iterates over all rows in all row groups.
     */
    public RowReader createRowReader() {
        return createRowReader(ColumnProjection.all());
    }

    /**
     * Create a RowReader that iterates over selected columns in all row groups.
     *
     * @param projection specifies which columns to read
     * @return a RowReader for the selected columns
     */
    public RowReader createRowReader(ColumnProjection projection) {
        FileSchema schema = getFileSchema();
        ProjectedSchema projectedSchema = ProjectedSchema.create(schema, projection);
        String fileName = path.getFileName().toString();
        return new SingleFileRowReader(schema, projectedSchema, channel, fileMetaData.rowGroups(), context, fileName);
    }

    @Override
    public void close() throws IOException {
        // Only close context if we created it
        // When opened via Hardwood, the context is closed when Hardwood is closed
        if (ownsContext) {
            context.close();
        }

        // Close channel
        channel.close();
    }
}
