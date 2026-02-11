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
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import dev.morling.hardwood.internal.thrift.FileMetaDataReader;
import dev.morling.hardwood.internal.thrift.ThriftCompactReader;
import dev.morling.hardwood.metadata.FileMetaData;

/**
 * Utility class for reading Parquet file metadata from a FileChannel.
 * <p>
 * This centralizes the metadata reading logic used by ParquetFileReader,
 * MultiFileRowReader, and CrossFilePrefetchCoordinator.
 * </p>
 */
public final class ParquetMetadataReader {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LENGTH_SIZE = 4;
    private static final int MAGIC_SIZE = 4;

    private ParquetMetadataReader() {
        // Utility class
    }

    /**
     * Reads file metadata from a Parquet file using an already-opened channel.
     *
     * @param channel the file channel to read from
     * @param path the file path (used for error messages)
     * @return the parsed FileMetaData
     * @throws IOException if the file is not a valid Parquet file or cannot be read
     */
    public static FileMetaData readMetadata(FileChannel channel, Path path) throws IOException {
        long fileSize = channel.size();
        if (fileSize < MAGIC_SIZE + MAGIC_SIZE + FOOTER_LENGTH_SIZE) {
            throw new IOException("File too small to be a valid Parquet file: " + path);
        }

        // Read and validate magic number at start
        ByteBuffer startMagicBuf = ByteBuffer.allocate(MAGIC_SIZE);
        readFully(channel, startMagicBuf, 0);
        if (!Arrays.equals(startMagicBuf.array(), MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at start): " + path);
        }

        // Read footer size and magic number at end
        long footerInfoPos = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE;
        ByteBuffer footerInfoBuf = ByteBuffer.allocate(FOOTER_LENGTH_SIZE + MAGIC_SIZE);
        readFully(channel, footerInfoBuf, footerInfoPos);
        footerInfoBuf.order(ByteOrder.LITTLE_ENDIAN);
        int footerLength = footerInfoBuf.getInt();
        byte[] endMagic = new byte[MAGIC_SIZE];
        footerInfoBuf.get(endMagic);
        if (!Arrays.equals(endMagic, MAGIC)) {
            throw new IOException("Not a Parquet file (invalid magic number at end): " + path);
        }

        // Validate footer length
        long footerStart = fileSize - MAGIC_SIZE - FOOTER_LENGTH_SIZE - footerLength;
        if (footerStart < MAGIC_SIZE) {
            throw new IOException("Invalid footer length: " + footerLength);
        }

        // Read footer
        ByteBuffer footerBuf = ByteBuffer.allocate(footerLength);
        readFully(channel, footerBuf, footerStart);

        // Parse file metadata
        ThriftCompactReader reader = new ThriftCompactReader(new ByteArrayInputStream(footerBuf.array()));
        return FileMetaDataReader.read(reader);
    }

    /**
     * Reads from channel at position until buffer is full.
     */
    private static void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position + buffer.position());
            if (read < 0) {
                throw new IOException("Unexpected end of file");
            }
        }
        buffer.flip();
    }
}
