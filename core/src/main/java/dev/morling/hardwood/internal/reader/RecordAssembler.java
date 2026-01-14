/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.internal.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import dev.morling.hardwood.schema.FieldPath;
import dev.morling.hardwood.schema.FileSchema;

/**
 * Independent column processing record assembler for Parquet data.
 *
 * <p>Each column is processed completely independently - the repetition levels alone
 * determine the exact "coordinates" (indices) where each value belongs in the nested
 * structure. Sibling columns are guaranteed to have parallel structure, so processing
 * them separately and merging by index is guaranteed to align correctly.</p>
 *
 * <h2>Algorithm</h2>
 * <pre>
 * For each column:
 *   indices = [0, 0, ...]  // One index per repetition level
 *   For each value in column (for current record):
 *     updateIndices(r)  // Compute position from rep level
 *     insertAtPath(record, path, indices, d, value)
 *
 * updateIndices(r):
 *   // Reset everything deeper than r to 0
 *   for i = r+1 to maxRepLevel: indices[i] = 0
 *   // Increment at level r (except r=0 which starts new record)
 *   if r > 0: indices[r]++
 * </pre>
 *
 * <h2>Example: [[1, 2], [3], [4, 5, 6]]</h2>
 * <pre>
 * Value | r | indices after  | Position
 * ------|---|----------------|----------
 *   1   | 0 | [0, 0]         | [0][0]
 *   2   | 2 | [0, 1]         | [0][1]
 *   3   | 1 | [1, 0]         | [1][0]
 *   4   | 1 | [2, 0]         | [2][0]
 *   5   | 2 | [2, 1]         | [2][1]
 *   6   | 2 | [2, 2]         | [2][2]
 * </pre>
 */
public class RecordAssembler {

    private final FileSchema schema;

    public RecordAssembler(FileSchema schema) {
        this.schema = schema;
    }

    /**
     * Assemble values from all columns into a row based on the schema structure.
     */
    public Object[] assembleRow(List<ColumnBatch> batches, int recordIndex) {
        int rootSize = schema.getRootNode().children().size();
        Object[] record = new Object[rootSize];

        // Process each column independently
        for (ColumnBatch batch : batches) {
            int colIndex = batch.getColumn().columnIndex();
            processColumn(batch, recordIndex, schema.getFieldPaths().get(colIndex), record);
        }

        return record;
    }

    /**
     * Process a single column, inserting all its values into the record.
     * Scans the batch to find values for the specified record (delimited by rep=0).
     */
    private void processColumn(ColumnBatch batch, int recordIndex, FieldPath path, Object[] record) {
        int maxRepLevel = batch.getColumn().maxRepetitionLevel();

        // Indices track position at each repetition level (like a multi-dimensional odometer)
        int[] indices = new int[maxRepLevel + 1];

        // Scan to find values for this record (records are delimited by rep=0)
        int currentRecord = 0;
        boolean first = true;

        for (int i = 0; i < batch.getValueCount(); i++) {
            int r = batch.getRepetitionLevel(i);

            // Check for record boundary (rep=0 after first value)
            if (!first && r == 0) {
                currentRecord++;
                if (currentRecord > recordIndex) {
                    break;  // Past our record, done
                }
            }
            first = false;

            // Process values belonging to our record
            if (currentRecord == recordIndex) {
                int d = batch.getDefinitionLevel(i);
                Object value = batch.getValue(i);

                // Update indices based on repetition level
                updateIndices(indices, r);

                // Insert value at the computed position
                insertAtPath(record, path, indices, d, value);
            }
        }
    }

    /**
     * Update indices based on repetition level.
     *
     * <p>The repetition level r means "repeating at level r":</p>
     * <ul>
     *   <li>Reset everything deeper than r to 0 (new element starts fresh)</li>
     *   <li>Increment at level r (except r=0 which starts a new record)</li>
     * </ul>
     */
    private void updateIndices(int[] indices, int r) {
        // Reset all levels deeper than r
        for (int i = r + 1; i < indices.length; i++) {
            indices[i] = 0;
        }

        // Increment at level r (except for r=0 which starts a new record)
        if (r > 0) {
            indices[r]++;
        }
    }

    /**
     * Insert a value into the record at the position determined by the path and indices.
     */
    private void insertAtPath(Object[] record, FieldPath path, int[] indices,
                               int defLevel, Object value) {
        Object current = record;
        int indexPtr = 0;

        FieldPath.PathStep[] steps = path.steps();
        for (int level = 0; level < steps.length; level++) {
            FieldPath.PathStep step = steps[level];

            // Stop if this level is not defined (NULL)
            if (step.definitionLevel() > defLevel) {
                return;
            }

            if (step.isList() || step.isMap()) {
                // Get or create list container at this struct field
                // (For nested lists, current may already be a List - skip in that case)
                if (current instanceof Object[] struct) {
                    @SuppressWarnings("unchecked")
                    List<Object> list = (List<Object>) struct[step.fieldIndex()];
                    if (list == null) {
                        list = new ArrayList<>();
                        struct[step.fieldIndex()] = list;
                    }
                    current = list;
                }

            } else if (step.isRepeated()) {
                // Navigate to element at current index within the list
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) current;
                int idx = indices[++indexPtr];

                if (step.numChildren() > 1) {
                    // Multi-field struct element (e.g., key_value in MAP)
                    ensureListSize(list, idx, () -> new Object[step.numChildren()]);
                    current = list.get(idx);
                } else {
                    // Determine element type from next step (if any)
                    boolean isLastStep = (level == steps.length - 1);
                    FieldPath.PathStep nextStep = isLastStep ? null : steps[level + 1];

                    if (isLastStep || nextStep.numChildren() == 0) {
                        // Primitive list element - set value directly
                        if (defLevel == path.maxDefLevel()) {
                            ensureListSize(list, idx, () -> null);
                            list.set(idx, value);
                        }
                        return;
                    } else if (nextStep.isList() || nextStep.isMap()) {
                        // Nested list
                        ensureListSize(list, idx, ArrayList::new);
                        current = list.get(idx);
                    } else {
                        // Struct element
                        ensureListSize(list, idx, () -> new Object[nextStep.numChildren()]);
                        current = list.get(idx);
                        level++;  // Skip next step - we've already created/navigated into it
                    }
                }

            } else if (step.numChildren() > 0) {
                // Navigate into struct field
                Object[] struct = (Object[]) current;
                Object child = struct[step.fieldIndex()];
                if (child == null) {
                    child = new Object[step.numChildren()];
                    struct[step.fieldIndex()] = child;
                }
                current = child;
            }
        }

        // Set leaf value (only if fully defined)
        if (defLevel == path.maxDefLevel()) {
            if (current instanceof Object[] struct) {
                struct[path.leafFieldIndex()] = value;
            }
        }
    }

    private void ensureListSize(List<Object> list, int idx, Supplier<Object> elementFactory) {
        while (list.size() <= idx) {
            list.add(elementFactory.get());
        }
    }
}
