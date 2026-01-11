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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import dev.morling.hardwood.internal.conversion.LogicalTypeConverter;
import dev.morling.hardwood.row.PqMap;
import dev.morling.hardwood.row.PqType;
import dev.morling.hardwood.schema.SchemaNode;

/**
 * Implementation of PqMap interface.
 */
public class PqMapImpl implements PqMap {

    private final List<Entry> entries;

    /**
     * Constructor for a map from assembled data.
     *
     * @param rawEntries list of Object[] where each array is [key, value]
     * @param mapSchema the MAP schema node
     */
    @SuppressWarnings("unchecked")
    public PqMapImpl(List<?> rawEntries, SchemaNode.GroupNode mapSchema) {
        if (rawEntries == null || rawEntries.isEmpty()) {
            this.entries = Collections.emptyList();
            return;
        }

        // Get key/value schemas from MAP -> key_value -> (key, value)
        SchemaNode.GroupNode keyValueGroup = (SchemaNode.GroupNode) mapSchema.children().get(0);
        SchemaNode keySchema = keyValueGroup.children().get(0);
        SchemaNode valueSchema = keyValueGroup.children().get(1);

        List<Entry> entryList = new ArrayList<>();
        for (Object rawEntry : rawEntries) {
            Object[] keyValuePair = (Object[]) rawEntry;
            entryList.add(new EntryImpl(keyValuePair[0], keyValuePair[1], keySchema, valueSchema));
        }
        this.entries = Collections.unmodifiableList(entryList);
    }

    @Override
    public List<Entry> getEntries() {
        return entries;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * Implementation of a map entry.
     */
    private static class EntryImpl implements Entry {

        private final Object key;
        private final Object value;
        private final SchemaNode keySchema;
        private final SchemaNode valueSchema;

        EntryImpl(Object key, Object value, SchemaNode keySchema, SchemaNode valueSchema) {
            this.key = key;
            this.value = value;
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K> K getKey(PqType<K> type) {
            return (K) convertValue(key, type, keySchema);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <V> V getValue(PqType<V> type) {
            if (value == null) {
                return null;
            }
            return (V) convertValue(value, type, valueSchema);
        }

        @Override
        public boolean isValueNull() {
            return value == null;
        }

        private Object convertValue(Object rawValue, PqType<?> type, SchemaNode fieldSchema) {
            if (rawValue == null) {
                return null;
            }

            // Handle primitive types
            if (type instanceof PqType.BooleanType) {
                return (Boolean) rawValue;
            }
            else if (type instanceof PqType.Int32Type) {
                return (Integer) rawValue;
            }
            else if (type instanceof PqType.Int64Type) {
                return (Long) rawValue;
            }
            else if (type instanceof PqType.FloatType) {
                return (Float) rawValue;
            }
            else if (type instanceof PqType.DoubleType) {
                return (Double) rawValue;
            }
            else if (type instanceof PqType.BinaryType) {
                return (byte[]) rawValue;
            }
            else if (type instanceof PqType.StringType) {
                if (rawValue instanceof String) {
                    return rawValue;
                }
                return new String((byte[]) rawValue, StandardCharsets.UTF_8);
            }
            else if (type instanceof PqType.DateType) {
                return convertLogicalType(rawValue, fieldSchema, LocalDate.class);
            }
            else if (type instanceof PqType.TimeType) {
                return convertLogicalType(rawValue, fieldSchema, LocalTime.class);
            }
            else if (type instanceof PqType.TimestampType) {
                return convertLogicalType(rawValue, fieldSchema, Instant.class);
            }
            else if (type instanceof PqType.DecimalType) {
                return convertLogicalType(rawValue, fieldSchema, BigDecimal.class);
            }
            else if (type instanceof PqType.UuidType) {
                return convertLogicalType(rawValue, fieldSchema, UUID.class);
            }
            else if (type instanceof PqType.RowType) {
                Object[] arrayValue = (Object[]) rawValue;
                SchemaNode.GroupNode groupSchema = (SchemaNode.GroupNode) fieldSchema;
                return new PqRowImpl(arrayValue, groupSchema);
            }
            else if (type instanceof PqType.ListType) {
                List<?> listValue = (List<?>) rawValue;
                SchemaNode.GroupNode listSchema = (SchemaNode.GroupNode) fieldSchema;
                return new PqListImpl(listValue, listSchema);
            }
            else if (type instanceof PqType.MapType) {
                List<?> mapValue = (List<?>) rawValue;
                SchemaNode.GroupNode mapSchemaGroup = (SchemaNode.GroupNode) fieldSchema;
                return new PqMapImpl(mapValue, mapSchemaGroup);
            }
            else {
                throw new IllegalArgumentException("Unknown PqType: " + type.getClass().getSimpleName());
            }
        }

        @SuppressWarnings("unchecked")
        private <T> T convertLogicalType(Object rawValue, SchemaNode fieldSchema, Class<T> expectedClass) {
            SchemaNode.PrimitiveNode primitive = (SchemaNode.PrimitiveNode) fieldSchema;
            Object converted = LogicalTypeConverter.convert(rawValue, primitive.type(), primitive.logicalType());
            return (T) converted;
        }
    }
}
