package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

public class SchemaDiff {
    private final List<TableModel> tableAdditions = new ArrayList<TableModel>();
    private final List<TableModel> tableDeletions = new ArrayList<TableModel>();
    private final List<TableDiff> tableDiff = new ArrayList<TableDiff>();
    public List<TableModel> getTableAdditions() {
        return tableAdditions;
    }

    public List<TableModel> getTableDeletions() {
        return tableDeletions;
    }
    public List<TableDiff> getTableDiff() {
        return tableDiff;
    }
}
