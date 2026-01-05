/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.compression;

import dev.morling.hardwood.metadata.CompressionCodec;

/**
 * Factory for creating decompressor instances based on compression codec.
 */
public class DecompressorFactory {

    private static final UncompressedDecompressor UNCOMPRESSED = new UncompressedDecompressor();
    private static final SnappyDecompressor SNAPPY = new SnappyDecompressor();
    private static final GzipDecompressor GZIP = new GzipDecompressor();
    private static final ZstdDecompressor ZSTD = new ZstdDecompressor();

    /**
     * Get a decompressor for the given compression codec.
     *
     * @param codec the compression codec
     * @return the appropriate decompressor
     * @throws UnsupportedOperationException if the codec is not supported
     */
    public static Decompressor getDecompressor(CompressionCodec codec) {
        return switch (codec) {
            case UNCOMPRESSED -> UNCOMPRESSED;
            case SNAPPY -> SNAPPY;
            case GZIP -> GZIP;
            case ZSTD -> ZSTD;
            case LZO -> throw new UnsupportedOperationException("LZO compression not yet supported");
            case BROTLI -> throw new UnsupportedOperationException("BROTLI compression not yet supported");
            case LZ4 -> throw new UnsupportedOperationException("LZ4 compression not yet supported");
            case LZ4_RAW -> throw new UnsupportedOperationException("LZ4_RAW compression not yet supported");
        };
    }
}
