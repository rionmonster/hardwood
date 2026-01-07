/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

import dev.morling.hardwood.row.PqType;

/**
 * Logical types that provide semantic meaning to physical types.
 * Sealed interface allows for parameterized types (e.g., DECIMAL with scale/precision).
 */
public sealed

interface LogicalType
permits LogicalType.StringType,LogicalType.EnumType,LogicalType.UuidType,LogicalType.IntType,LogicalType.DecimalType,LogicalType.DateType,LogicalType.TimeType,LogicalType.TimestampType,LogicalType.IntervalType,LogicalType.JsonType,LogicalType.BsonType,LogicalType.ListType,LogicalType.MapType
{

    /**
     * Returns the corresponding PqType for this logical type, or null if not supported.
     */
    default PqType<?> toPqType() {
        return null;
    }

    // Simple types (no parameters)
    record StringType() implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.STRING;
        }}

    record EnumType() implements LogicalType {}

    record UuidType() implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.UUID;
        }}

    record DateType() implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.DATE;
        }}

    record JsonType() implements LogicalType {}

    record BsonType() implements LogicalType {}

    record IntervalType() implements LogicalType {}

    // Parameterized: Integer types with bitWidth and sign
    record IntType(int bitWidth, boolean isSigned) implements LogicalType {
        public IntType {
            if (bitWidth != 8 && bitWidth != 16 && bitWidth != 32 && bitWidth != 64) {
                throw new IllegalArgumentException("Invalid bit width: " + bitWidth);
            }
        }

    @Override
        public PqType<?> toPqType() {
            return bitWidth == 64 ? PqType.INT64 : PqType.INT32;
        }}

    // Parameterized: Decimal with scale and precision
    record DecimalType(int scale, int precision) implements LogicalType {
        public DecimalType {
            if (precision <= 0) {
                throw new IllegalArgumentException("Precision must be positive: " + precision);
            }
            if (scale < 0) {
                throw new IllegalArgumentException("Scale cannot be negative: " + scale);
            }
        }

    @Override
        public PqType<?> toPqType() {
            return PqType.DECIMAL;
        }}

    // Parameterized: Time with unit and UTC adjustment
    record TimeType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.TIME;
        }}

    // Parameterized: Timestamp with unit and UTC adjustment
    record TimestampType(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.TIMESTAMP;
        }}

    // Complex types (not fully supported in Milestone 1)
    record ListType() implements LogicalType {

    @Override
        public PqType<?> toPqType() {
            return PqType.LIST;
        }}

    record MapType() implements LogicalType {}

    public enum TimeUnit { MILLIS, MICROS, NANOS }
}
