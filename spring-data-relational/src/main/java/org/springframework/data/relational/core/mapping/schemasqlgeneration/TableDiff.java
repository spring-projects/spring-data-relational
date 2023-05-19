package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

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
