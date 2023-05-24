package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to keep track of columns that have been added or deleted,
 * when performing a difference between a source and target {@link SchemaModel}
 *
 * @author Kurt Niemi
 * @since 3.2
 */
public class TableDiff {
    private final TableModel tableModel;
    private final List<ColumnModel> addedColumns = new ArrayList<ColumnModel>();
    private final List<ColumnModel> deletedColumns = new ArrayList<ColumnModel>();

    public TableDiff(TableModel tableModel) {

        this.tableModel = tableModel;
    }

    public TableModel getTableModel() {

        return tableModel;
    }

    public List<ColumnModel> getAddedColumns() {

        return addedColumns;
    }

    public List<ColumnModel> getDeletedColumns() {

        return deletedColumns;
    }
}
