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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import dev.morling.hardwood.internal.compression.Decompressor;
import dev.morling.hardwood.internal.compression.DecompressorFactory;
import dev.morling.hardwood.internal.encoding.ByteStreamSplitDecoder;
import dev.morling.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.morling.hardwood.internal.encoding.DeltaByteArrayDecoder;
import dev.morling.hardwood.internal.encoding.DeltaLengthByteArrayDecoder;
import dev.morling.hardwood.internal.encoding.PlainDecoder;
import dev.morling.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.morling.hardwood.internal.thrift.PageHeaderReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnMetaData;
import dev.morling.hardwood.metadata.DataPageHeader;
import dev.morling.hardwood.metadata.DataPageHeaderV2;
import dev.morling.hardwood.metadata.Encoding;
import dev.morling.hardwood.metadata.PageHeader;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.schema.ColumnSchema;

/**
 * Decoder for individual Parquet data pages.
 * <p>
 * This class provides stateless page decoding via {@link #decodeSinglePage}.
 * Page scanning and dictionary parsing are handled by {@link PageScanner}.
 * </p>
 */
public class PageReader {

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final Dictionary dictionary;

    /**
     * Constructor for single-page decoding with a pre-parsed dictionary.
     */
    private PageReader(ColumnMetaData columnMetaData, ColumnSchema column, Dictionary dictionary) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.dictionary = dictionary;
    }

    /**
     * Decode a single data page from a buffer.
     * <p>
     * The buffer should contain the complete page including header.
     * This method is stateless and thread-safe.
     * </p>
     *
     * @param pageBuffer buffer containing just this page (header + data)
     * @param columnMetaData metadata for the column
     * @param column column schema
     * @param dictionary pre-parsed dictionary, or null if not dictionary-encoded
     * @return decoded page
     */
    public static Page decodeSinglePage(ByteBuffer pageBuffer, ColumnMetaData columnMetaData,
                                        ColumnSchema column, Dictionary dictionary) throws IOException {
        // Parse page header
        ByteBufferInputStream headerStream = new ByteBufferInputStream(pageBuffer, 0);
        ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerStream.getBytesRead();

        // Read page data
        int compressedSize = pageHeader.compressedPageSize();
        byte[] pageData = new byte[compressedSize];
        pageBuffer.slice(headerSize, compressedSize).get(pageData);

        // Create a temporary PageReader instance for parsing
        PageReader reader = new PageReader(columnMetaData, column, dictionary);

        return switch (pageHeader.type()) {
            case DATA_PAGE -> {
                Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
                byte[] uncompressedData = decompressor.decompress(pageData, pageHeader.uncompressedPageSize());
                yield reader.parseDataPage(pageHeader.dataPageHeader(), uncompressedData);
            }
            case DATA_PAGE_V2 -> {
                yield reader.parseDataPageV2(pageHeader.dataPageHeaderV2(), pageData, pageHeader.uncompressedPageSize());
            }
            default -> throw new IOException("Unexpected page type for single-page decode: " + pageHeader.type());
        };
    }

    /**
     * Decode levels using RLE/Bit-Packing Hybrid encoding.
     */
    private int[] decodeLevels(byte[] levelData, int numValues, int maxLevel) throws IOException {
        int[] levels = new int[numValues];
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                new ByteArrayInputStream(levelData), getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /**
     * Count non-null values based on definition levels.
     */
    private int countNonNullValues(int numValues, int[] definitionLevels) {
        if (definitionLevels == null) {
            return numValues;
        }
        int maxDefLevel = column.maxDefinitionLevel();
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels[i] == maxDefLevel) {
                count++;
            }
        }
        return count;
    }

    /**
     * Read a 4-byte little-endian integer from the stream.
     */
    private int readLittleEndianInt(InputStream stream) throws IOException {
        byte[] bytes = new byte[4];
        stream.read(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    /**
     * InputStream that reads from a ByteBuffer at a given offset.
     * Tracks bytes read for determining header size.
     */
    static class ByteBufferInputStream extends InputStream {
        private final ByteBuffer buffer;
        private final int startOffset;
        private int pos;

        public ByteBufferInputStream(ByteBuffer buffer, int startOffset) {
            this.buffer = buffer;
            this.startOffset = startOffset;
            this.pos = startOffset;
        }

        @Override
        public int read() {
            if (pos >= buffer.limit()) {
                return -1;
            }
            return buffer.get(pos++) & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= buffer.limit()) {
                return -1;
            }
            int available = Math.min(len, buffer.limit() - pos);
            buffer.slice(pos, available).get(b, off, available);
            pos += available;
            return available;
        }

        public int getBytesRead() {
            return pos - startOffset;
        }
    }

    private Page parseDataPage(DataPageHeader header, byte[] data) throws IOException {
        ByteArrayInputStream dataStream = new ByteArrayInputStream(data);

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            int repLevelLength = readLittleEndianInt(dataStream);
            byte[] repLevelData = new byte[repLevelLength];
            dataStream.read(repLevelData);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0) {
            int defLevelLength = readLittleEndianInt(dataStream);
            byte[] defLevelData = new byte[defLevelLength];
            dataStream.read(defLevelData);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        int numNonNullValues = countNonNullValues(header.numValues(), definitionLevels);

        return decodeTypedValues(
                header.encoding(), dataStream, header.numValues(), numNonNullValues,
                definitionLevels, repetitionLevels);
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, byte[] pageData, int uncompressedPageSize)
            throws IOException {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.length - valuesOffset;

        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            byte[] repLevelData = new byte[repLevelLen];
            System.arraycopy(pageData, 0, repLevelData, 0, repLevelLen);
            repetitionLevels = decodeLevels(repLevelData, header.numValues(), column.maxRepetitionLevel());
        }

        int[] definitionLevels = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            byte[] defLevelData = new byte[defLevelLen];
            System.arraycopy(pageData, repLevelLen, defLevelData, 0, defLevelLen);
            definitionLevels = decodeLevels(defLevelData, header.numValues(), column.maxDefinitionLevel());
        }

        byte[] valuesData;
        int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;

        if (header.isCompressed() && compressedValuesLen > 0) {
            byte[] compressedValues = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, compressedValues, 0, compressedValuesLen);
            Decompressor decompressor = DecompressorFactory.getDecompressor(columnMetaData.codec());
            valuesData = decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        else {
            valuesData = new byte[compressedValuesLen];
            System.arraycopy(pageData, valuesOffset, valuesData, 0, compressedValuesLen);
        }

        ByteArrayInputStream valuesStream = new ByteArrayInputStream(valuesData);
        int numNonNullValues = header.numValues() - header.numNulls();

        return decodeTypedValues(
                header.encoding(), valuesStream, header.numValues(), numNonNullValues,
                definitionLevels, repetitionLevels);
    }

    /**
     * Decode values into Page using primitive arrays where possible.
     */
    private Page decodeTypedValues(Encoding encoding, InputStream dataStream,
                                   int numValues, int numNonNullValues,
                                   int[] definitionLevels, int[] repetitionLevels) throws IOException {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(dataStream, type, column.typeLength());
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BOOLEAN -> {
                        boolean[] values = new boolean[numValues];
                        decoder.readBooleans(values, definitionLevels, maxDefLevel);
                        yield new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case DELTA_BINARY_PACKED -> {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(dataStream);
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw new UnsupportedOperationException(
                            "DELTA_BINARY_PACKED not supported for type: " + type);
                };
            }
            case BYTE_STREAM_SPLIT -> {
                byte[] allData = dataStream.readAllBytes();
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        allData, numNonNullValues, type, column.typeLength());
                return switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FIXED_LEN_BYTE_ARRAY -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw new UnsupportedOperationException(
                            "BYTE_STREAM_SPLIT not supported for type: " + type);
                };
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IOException("Dictionary page not found for " + encoding + " encoding");
                }
                int bitWidth = dataStream.read();
                if (bitWidth < 0) {
                    throw new IOException("Failed to read bit width for dictionary indices");
                }
                byte[] indicesData = dataStream.readAllBytes();
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(indicesData), bitWidth);

                // Use typed dictionary directly
                return switch (dictionary) {
                    case Dictionary.LongDictionary d -> {
                        long[] values = new long[numValues];
                        indexDecoder.readDictionaryLongs(values, d.values(), definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case Dictionary.DoubleDictionary d -> {
                        double[] values = new double[numValues];
                        indexDecoder.readDictionaryDoubles(values, d.values(), definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case Dictionary.IntDictionary d -> {
                        int[] values = new int[numValues];
                        indexDecoder.readDictionaryInts(values, d.values(), definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case Dictionary.FloatDictionary d -> {
                        float[] values = new float[numValues];
                        indexDecoder.readDictionaryFloats(values, d.values(), definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case Dictionary.ByteArrayDictionary d -> {
                        byte[][] values = new byte[numValues][];
                        indexDecoder.readDictionaryByteArrays(values, d.values(), definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                };
            }
            case RLE -> {
                // RLE encoding for boolean values uses bit-width of 1
                if (type != PhysicalType.BOOLEAN) {
                    throw new UnsupportedOperationException(
                            "RLE encoding for non-boolean types not yet supported: " + type);
                }

                // Read 4-byte length prefix (little-endian)
                byte[] lengthBytes = new byte[4];
                dataStream.read(lengthBytes);
                int rleLength = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                // Read the RLE-encoded data
                byte[] rleData = new byte[rleLength];
                dataStream.read(rleData);

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(
                        new ByteArrayInputStream(rleData), 1);
                boolean[] values = new boolean[numValues];
                decoder.readBooleans(values, definitionLevels, maxDefLevel);
                return new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_BYTE_ARRAY -> {
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(dataStream);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                return new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            default -> throw new UnsupportedOperationException("Encoding not yet supported: " + encoding);
        }
    }
}
