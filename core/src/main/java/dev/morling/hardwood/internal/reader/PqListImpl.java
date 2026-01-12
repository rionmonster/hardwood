/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.metadata.LogicalType;
import dev.morling.hardwood.metadata.PhysicalType;
import dev.morling.hardwood.row.PqList;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqList interface.
 */
public class PqListImpl implements PqList {

    private final List<?> elements;
    private final SchemaNode.GroupNode listSchema;
    private final SchemaNode elementSchema;

    public PqListImpl(List<?> elements, SchemaNode.GroupNode listSchema) {
        this.elements = elements;
        this.listSchema = listSchema;
        this.elementSchema = listSchema.getListElement();
    }

    @Override
    public <T> Iterable<T> getValues(PqType<T> elementType) {
        validateElementType(elementType);
        return () -> new ConvertingIterator<>(elements.iterator(), elementType);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    private void validateElementType(PqType<?> type) {
        if (elementSchema == null) {
            throw new IllegalStateException("List has no element schema");
        }

        switch (type) {
            case PqType.BooleanType t -> validatePrimitiveElement(PhysicalType.BOOLEAN);
            case PqType.Int32Type t -> validatePrimitiveElement(PhysicalType.INT32);
            case PqType.Int64Type t -> validatePrimitiveElement(PhysicalType.INT64);
            case PqType.FloatType t -> validatePrimitiveElement(PhysicalType.FLOAT);
            case PqType.DoubleType t -> validatePrimitiveElement(PhysicalType.DOUBLE);
            case PqType.BinaryType t -> validatePrimitiveElement(PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
            case PqType.StringType t -> validateStringElement();
            case PqType.DateType t -> validateLogicalElement(LogicalType.DateType.class);
            case PqType.TimeType t -> validateLogicalElement(LogicalType.TimeType.class);
            case PqType.TimestampType t -> validateLogicalElement(LogicalType.TimestampType.class);
            case PqType.DecimalType t -> validateLogicalElement(LogicalType.DecimalType.class);
            case PqType.UuidType t -> validateLogicalElement(LogicalType.UuidType.class);
            case PqType.RowType t -> validateGroupElement(false, false);
            case PqType.ListType t -> validateGroupElement(true, false);
            case PqType.MapType t -> validateGroupElement(false, true);
        }
    }

    private void validatePrimitiveElement(PhysicalType... expectedTypes) {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        for (PhysicalType expected : expectedTypes) {
            if (primitive.type() == expected) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "List elements have physical type " + primitive.type()
                        + ", expected one of " + java.util.Arrays.toString(expectedTypes));
    }

    private void validateStringElement() {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        // Accept BYTE_ARRAY with or without STRING logical type
        if (primitive.type() != PhysicalType.BYTE_ARRAY) {
            throw new IllegalArgumentException(
                    "List elements have physical type " + primitive.type() + ", expected BYTE_ARRAY for STRING");
        }
    }

    private void validateLogicalElement(Class<? extends LogicalType> expectedType) {
        if (!(elementSchema instanceof SchemaNode.PrimitiveNode primitive)) {
            throw new IllegalArgumentException("List elements are not primitive types");
        }
        LogicalType logicalType = primitive.logicalType();
        if (logicalType == null || !expectedType.isInstance(logicalType)) {
            throw new IllegalArgumentException(
                    "List elements have logical type " + (logicalType == null ? "none" : logicalType.getClass().getSimpleName())
                            + ", expected " + expectedType.getSimpleName());
        }
    }

    private void validateGroupElement(boolean expectList, boolean expectMap) {
        if (!(elementSchema instanceof SchemaNode.GroupNode group)) {
            throw new IllegalArgumentException("List elements are not group types");
        }
        if (expectList && !group.isList()) {
            throw new IllegalArgumentException("List elements are not lists");
        }
        if (expectMap && !group.isMap()) {
            throw new IllegalArgumentException("List elements are not maps");
        }
        if (!expectList && !expectMap && (group.isList() || group.isMap())) {
            throw new IllegalArgumentException("List elements are lists or maps, not structs");
        }
    }

    private class ConvertingIterator<T> implements Iterator<T> {
        private final Iterator<?> delegate;
        private final PqType<T> type;

        ConvertingIterator(Iterator<?> delegate, PqType<T> type) {
            this.delegate = delegate;
            this.type = type;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            Object rawValue = delegate.next();
            if (rawValue == null) {
                return null;
            }
            return (T) convertElement(rawValue, type);
        }
    }

    private Object convertElement(Object rawValue, PqType<?> type) {
        return switch (type) {
            case PqType.BooleanType t -> rawValue;
            case PqType.Int32Type t -> rawValue;
            case PqType.Int64Type t -> rawValue;
            case PqType.FloatType t -> rawValue;
            case PqType.DoubleType t -> rawValue;
            case PqType.BinaryType t -> rawValue;
            case PqType.StringType t -> rawValue instanceof String ? rawValue : new String((byte[]) rawValue, StandardCharsets.UTF_8);
            case PqType.DateType t -> convertLogicalElement(rawValue, LocalDate.class);
            case PqType.TimeType t -> convertLogicalElement(rawValue, LocalTime.class);
            case PqType.TimestampType t -> convertLogicalElement(rawValue, Instant.class);
            case PqType.DecimalType t -> convertLogicalElement(rawValue, BigDecimal.class);
            case PqType.UuidType t -> convertLogicalElement(rawValue, UUID.class);
            case PqType.RowType t -> new PqRowImpl((Object[]) rawValue, (SchemaNode.GroupNode) elementSchema);
            case PqType.ListType t -> new PqListImpl((List<?>) rawValue, (SchemaNode.GroupNode) elementSchema);
            case PqType.MapType t -> new PqMapImpl((List<?>) rawValue, (SchemaNode.GroupNode) elementSchema);
        };
    }

    private <T> T convertLogicalElement(Object rawValue, Class<T> expectedClass) {
        // If already converted (e.g., by RecordAssembler for nested lists), return as-is
        if (expectedClass.isInstance(rawValue)) {
            return expectedClass.cast(rawValue);
        }
        SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) elementSchema;
        Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
        return expectedClass.cast(converted);
    }
}
