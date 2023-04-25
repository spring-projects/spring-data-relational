package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import org.springframework.data.relational.core.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;

public class TableDiff {
    private final TableModel table;
    private final List<ColumnModel> addedColumns = new ArrayList<ColumnModel>();
    private final List<ColumnModel> deletedColumns = new ArrayList<ColumnModel>();

    public TableDiff(TableModel table) {
        this.table = table;
    }

    public TableModel getTable() {
        return table;
    }

    public List<ColumnModel> getAddedColumns() {
        return addedColumns;
    }

    public List<ColumnModel> getDeletedColumns() {
        return deletedColumns;
    }
}
