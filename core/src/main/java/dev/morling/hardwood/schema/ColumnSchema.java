/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.schema;

import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.metadata.RepetitionType;
import dev.morling.hardwood.row.PqType;

/**
 * Represents a primitive column in a Parquet schema. Stores computed definition
 * and repetition levels based on schema hierarchy.
 */
public record ColumnSchema(
        String name,
        PhysicalType type,
        RepetitionType repetitionType,
        Integer typeLength,
        int columnIndex,
        int maxDefinitionLevel,
        int maxRepetitionLevel,
        LogicalType logicalType) {

    /**
     * Returns the corresponding PqType for this column based on its logical and
     * physical types.
     */
    public PqType<?> toPqType() {
        if (logicalType != null) {
            return switch (logicalType) {
            case LogicalType.StringType t -> PqType.STRING;
            case LogicalType.UuidType t -> PqType.UUID;
            case LogicalType.DateType t -> PqType.DATE;
            case LogicalType.TimeType t -> PqType.TIME;
            case LogicalType.TimestampType t -> PqType.TIMESTAMP;
            case LogicalType.DecimalType t -> PqType.DECIMAL;
            case LogicalType.IntType intType -> intType.bitWidth() == 64 ? PqType.INT64 : PqType.INT32;
            case LogicalType.ListType t -> PqType.LIST;
            default -> toPqTypeFromPhysical();
            };
        }
        return toPqTypeFromPhysical();
    }

    private PqType<?> toPqTypeFromPhysical() {
        return switch (type) {
        case BOOLEAN -> PqType.BOOLEAN;
        case INT32 -> PqType.INT32;
        case INT64 -> PqType.INT64;
        case FLOAT -> PqType.FLOAT;
        case DOUBLE -> PqType.DOUBLE;
        case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> PqType.BINARY;
        default -> PqType.BINARY;
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(repetitionType.name().toLowerCase());
        sb.append(" ");
        sb.append(type.name().toLowerCase());
        if (typeLength != null) {
            sb.append("(").append(typeLength).append(")");
        }
        sb.append(" ");
        sb.append(name);
        if (logicalType != null) {
            sb.append(" (").append(logicalType).append(")");
        }
        sb.append(";");
        return sb.toString();
    }
}
