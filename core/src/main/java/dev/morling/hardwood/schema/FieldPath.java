/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.schema;

/**
 * Pre-computed path from root to a primitive column.
 *
 * @param steps Path steps from root to the parent of this column
 * @param leafFieldIndex Index of this column's field in its parent struct
 * @param maxDefLevel Maximum definition level for this column
 * @param maxRepLevel Maximum repetition level for this column
 */
public record FieldPath(
        PathStep[] steps,
        int leafFieldIndex,
        int maxDefLevel,
        int maxRepLevel
) {

    /**
     * A single step in the path from root to a column.
     *
     * @param fieldIndex Index of this field in parent struct
     * @param definitionLevel Definition level at this node
     * @param repetitionLevel Repetition level at this node
     * @param isRepeated True if this node has REPEATED repetition type
     * @param isList True if this node is a LIST container
     * @param isMap True if this node is a MAP container
     * @param numChildren Number of children (for struct allocation); 0 for primitives
     */
    public record PathStep(
            int fieldIndex,
            int definitionLevel,
            int repetitionLevel,
            boolean isRepeated,
            boolean isList,
            boolean isMap,
            int numChildren
    ) {
        /**
         * True if this step represents a container (struct, list, or map).
         */
        public boolean isContainer() {
            return numChildren > 0 || isList || isMap;
        }
    }
}
