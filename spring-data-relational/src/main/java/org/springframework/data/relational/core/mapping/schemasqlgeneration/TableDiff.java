package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to keep track of columns that have been added or deleted,
 * when performing a difference between a source and target {@link SchemaModel}
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public record TableDiff(TableModel tableModel,
                        ArrayList<ColumnModel> addedColumns,
                        ArrayList<ColumnModel> deletedColumns) {

    public TableDiff(TableModel tableModel) {
        this(tableModel, new ArrayList<>(), new ArrayList<>());
    }

}
