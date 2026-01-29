/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import dev.morling.hardwood.internal.compression.Decompressor;
import dev.morling.hardwood.internal.compression.DecompressorFactory;
import dev.morling.hardwood.internal.reader.Page;
import dev.morling.hardwood.internal.reader.PageInfo;
import dev.morling.hardwood.internal.reader.PageReader;
import dev.morling.hardwood.internal.reader.PageScanner;
import dev.morling.hardwood.internal.thrift.PageHeaderReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.ColumnChunk;
import dev.morling.hardwood.metadata.PageHeader;
import dev.morling.hardwood.metadata.RowGroup;
import dev.morling.hardwood.reader.ParquetFileReader;
import dev.morling.hardwood.schema.ColumnSchema;
import dev.morling.hardwood.schema.FileSchema;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgs = { "-Xms1g", "-Xmx1g" })
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class PageDecompressionBenchmark {

    @Param({})
    private String dataDir;

    @Param("yellow_tripdata_2025-05.parquet")
    private String fileName;

    private Path path;
    private FileChannel channel;
    private List<PageInfo> allPages;

    @Setup
    public void setup() throws IOException {
        path = Path.of(dataDir).resolve(fileName).toAbsolutePath().normalize();
        if (!path.toFile().exists()) {
            throw new IllegalStateException("Parquet file not found: " + path +
                    ". Run './mvnw verify -Pperformance-test' first to download test data.");
        }

        channel = FileChannel.open(path, StandardOpenOption.READ);
        allPages = new ArrayList<>();

        // Scan all pages from all columns in all row groups
        try (ParquetFileReader reader = ParquetFileReader.open(path)) {
            FileSchema schema = reader.getFileSchema();
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();

            for (RowGroup rowGroup : rowGroups) {
                for (int colIdx = 0; colIdx < rowGroup.columns().size(); colIdx++) {
                    ColumnChunk columnChunk = rowGroup.columns().get(colIdx);
                    ColumnSchema columnSchema = schema.getColumn(colIdx);

                    PageScanner scanner = new PageScanner(channel, columnSchema, columnChunk);
                    allPages.addAll(scanner.scanPages());
                }
            }
        }

        System.out.println("Scanned " + allPages.size() + " pages from " + path.getFileName());
    }

    @TearDown
    public void tearDown() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    @Benchmark
    public void decompressPages(Blackhole blackhole) throws IOException {
        for (PageInfo pageInfo : allPages) {
            MappedByteBuffer pageData = pageInfo.pageData();

            // Parse page header to get compressed/uncompressed sizes
            ByteBufferInputStream headerStream = new ByteBufferInputStream(pageData, 0);
            ThriftCompactReader headerReader = new ThriftCompactReader(headerStream);
            PageHeader header = PageHeaderReader.read(headerReader);
            int headerSize = headerStream.getBytesRead();

            int compressedSize = header.compressedPageSize();
            int uncompressedSize = header.uncompressedPageSize();

            // Slice compressed data
            MappedByteBuffer compressedData = pageData.slice(headerSize, compressedSize);

            // Decompress using the file's actual codec
            Decompressor decompressor = DecompressorFactory.getDecompressor(pageInfo.columnMetaData().codec());
            byte[] decompressed = decompressor.decompress(compressedData, uncompressedSize);
            blackhole.consume(decompressed);
        }
    }

    @Benchmark
    public void decodePages(Blackhole blackhole) throws IOException {
        for (PageInfo pageInfo : allPages) {
            PageReader pageReader = new PageReader(pageInfo.columnMetaData(), pageInfo.columnSchema());
            Page page = pageReader.decodePage(pageInfo.pageData(), pageInfo.dictionary());
            blackhole.consume(page);
        }
    }

    /**
     * InputStream that reads from a ByteBuffer at a given offset.
     */
    static class ByteBufferInputStream extends InputStream {
        private final MappedByteBuffer buffer;
        private final int startOffset;
        private int pos;

        public ByteBufferInputStream(MappedByteBuffer buffer, int startOffset) {
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
}
