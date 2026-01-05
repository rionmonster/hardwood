/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.row;

/**
 * Represents a single row in a Parquet file with typed accessor methods.
 */
public interface Row {

    // Boolean accessors
    boolean getBoolean(int position);

    boolean getBoolean(String name);

    // INT32 accessors
    int getInt(int position);

    int getInt(String name);

    // INT64 accessors
    long getLong(int position);

    long getLong(String name);

    // FLOAT accessors
    float getFloat(int position);

    float getFloat(String name);

    // DOUBLE accessors
    double getDouble(int position);

    double getDouble(String name);

    // BYTE_ARRAY accessors
    byte[] getByteArray(int position);

    byte[] getByteArray(String name);

    // String convenience (BYTE_ARRAY as UTF-8)
    String getString(int position);

    String getString(String name);

    // Null checking
    boolean isNull(int position);

    boolean isNull(String name);

    // Metadata
    int getColumnCount();

    String getColumnName(int position);
}
