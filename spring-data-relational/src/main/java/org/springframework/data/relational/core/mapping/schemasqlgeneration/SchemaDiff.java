package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is created to return the difference between a source and target {@link SchemaModel}
 *
 * The difference consists of Table Additions, Deletions, and Modified Tables (i.e. table
 * exists in both source and target - but has columns to add or delete)
 *
 * @author Kurt Niemi
 * @since 3.2
 */
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
