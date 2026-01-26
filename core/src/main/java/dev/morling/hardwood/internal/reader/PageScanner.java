/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.internal.compression.Decompressor;
import dev.morling.hardwood.internal.compression.DecompressorFactory;
import dev.morling.hardwood.internal.encoding.PlainDecoder;
import dev.morling.hardwood.internal.thrift.PageHeaderReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.metadata.CompressionCodec;
import dev.morling.hardwood.metadata.PageHeader;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Scans page boundaries in column chunks and creates PageInfo objects.
 * <p>
 * Reads page headers and parses dictionary pages upfront, then creates
 * PageInfo records that can be used for on-demand page decoding.
 * </p>
 */
public class PageScanner {

    private final FileChannel channel;
    private final FileSchema schema;
    private final List<RowGroup> rowGroups;

    public PageScanner(FileChannel channel, FileSchema schema, List<RowGroup> rowGroups) {
        this.channel = channel;
        this.schema = schema;
        this.rowGroups = rowGroups;
    }

    /**
     * Scan all pages and return PageInfo lists for each column.
     * Does not decode pages - just collects metadata and buffer slices.
     *
     * @return list of PageInfo lists, one per column
     */
    public List<List<PageInfo>> scanPages() throws IOException {
        int columnCount = schema.getColumnCount();
        List<List<PageInfo>> pageInfosByColumn = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            pageInfosByColumn.add(new ArrayList<>());
        }

        for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
            RowGroup rowGroup = rowGroups.get(rgIndex);

            for (int colIndex = 0; colIndex < columnCount; colIndex++) {
                ColumnSchema columnSchema = schema.getColumn(colIndex);
                ColumnChunk columnChunk = rowGroup.columns().get(colIndex);
                ColumnMetaData metaData = columnChunk.metaData();

                Long dictOffset = metaData.dictionaryPageOffset();
                long chunkStartOffset = (dictOffset != null && dictOffset > 0)
                        ? dictOffset
                        : metaData.dataPageOffset();
                long chunkSize = metaData.totalCompressedSize();

                // Map the column chunk and keep reference to prevent GC
                MappedByteBuffer buffer = channel.map(
                    FileChannel.MapMode.READ_ONLY, chunkStartOffset, chunkSize);

                // Scan pages and create PageInfo objects with buffer slices
                List<PageInfo> pageInfos = scanChunkPages(buffer, columnSchema, metaData);
                pageInfosByColumn.get(colIndex).addAll(pageInfos);
            }
        }

        return pageInfosByColumn;
    }

    /**
     * Scan a column chunk and create PageInfo objects for each data page.
     * Dictionary pages are parsed upfront and the parsed dictionary is shared
     * with all data page PageInfo objects.
     * <p>
     * Each PageInfo receives a ByteBuffer slice of the pre-mapped chunk, avoiding
     * per-page memory mapping overhead.
     * </p>
     */
    private List<PageInfo> scanChunkPages(MappedByteBuffer buffer,
            ColumnSchema columnSchema, ColumnMetaData metaData) throws IOException {

        List<PageInfo> pageInfos = new ArrayList<>();
        long valuesRead = 0;
        int position = 0;

        // Dictionary (parsed upfront if present)
        Dictionary dictionary = null;

        while (valuesRead < metaData.numValues() && position < buffer.limit()) {
            // Read page header
            PageReader.ByteBufferInputStream headerStream =
                new PageReader.ByteBufferInputStream(buffer, position);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerStream.getBytesRead();

            int pageDataOffset = position + headerSize;
            int compressedSize = header.compressedPageSize();
            int totalPageSize = headerSize + compressedSize;

            if (header.type() == PageHeader.PageType.DICTIONARY_PAGE) {
                // Parse dictionary page upfront
                byte[] compressedData = new byte[compressedSize];
                buffer.slice(pageDataOffset, compressedSize).get(compressedData);
                int numValues = header.dictionaryPageHeader().numValues();
                int uncompressedSize = header.uncompressedPageSize();

                dictionary = parseDictionary(compressedData, numValues, uncompressedSize,
                    columnSchema, metaData.codec());
            }
            else if (header.type() == PageHeader.PageType.DATA_PAGE ||
                     header.type() == PageHeader.PageType.DATA_PAGE_V2) {
                // Create a slice of the buffer for this page (includes header)
                // The slice shares the underlying memory mapping
                ByteBuffer pageSlice = buffer.slice(position, totalPageSize);

                PageInfo pageInfo = new PageInfo(
                    pageSlice,
                    columnSchema,
                    metaData,
                    dictionary
                );
                pageInfos.add(pageInfo);

                valuesRead += getValueCount(header);
            }

            // Move to next page
            position += totalPageSize;
        }

        return pageInfos;
    }

    /**
     * Parse a dictionary page into a Dictionary object.
     */
    private Dictionary parseDictionary(byte[] compressedData, int numValues,
            int uncompressedSize, ColumnSchema column, CompressionCodec codec) throws IOException {

        // Decompress dictionary data
        Decompressor decompressor = DecompressorFactory.getDecompressor(codec);
        byte[] data = decompressor.decompress(compressedData, uncompressedSize);

        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);
        PlainDecoder decoder = new PlainDecoder(dataStream, column.type(), column.typeLength());

        return switch (column.type()) {
            case INT32 -> {
                int[] values = new int[numValues];
                decoder.readInts(values, null, 0);
                yield new Dictionary.IntDictionary(values);
            }
            case INT64 -> {
                long[] values = new long[numValues];
                decoder.readLongs(values, null, 0);
                yield new Dictionary.LongDictionary(values);
            }
            case FLOAT -> {
                float[] values = new float[numValues];
                decoder.readFloats(values, null, 0);
                yield new Dictionary.FloatDictionary(values);
            }
            case DOUBLE -> {
                double[] values = new double[numValues];
                decoder.readDoubles(values, null, 0);
                yield new Dictionary.DoubleDictionary(values);
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, null, 0);
                yield new Dictionary.ByteArrayDictionary(values);
            }
            case BOOLEAN -> throw new UnsupportedOperationException(
                    "Dictionary encoding not supported for BOOLEAN type");
        };
    }

    private long getValueCount(PageHeader header) {
        return switch (header.type()) {
            case DATA_PAGE -> header.dataPageHeader().numValues();
            case DATA_PAGE_V2 -> header.dataPageHeaderV2().numValues();
            case DICTIONARY_PAGE -> 0;
            case INDEX_PAGE -> 0;
        };
    }
}
