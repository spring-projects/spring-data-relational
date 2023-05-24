package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.*;

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
    private final List<TableDiff> tableDiffs = new ArrayList<TableDiff>();

    private SchemaModel source;
    private SchemaModel target;

    /**
     *
     * Compare two {@link SchemaModel} to identify differences.
     *
     * @param target - Model reflecting current database state
     * @param source - Model reflecting desired database state
     */
    public SchemaDiff(SchemaModel target, SchemaModel source) {

        this.source = source;
        this.target = target;

        diffTableAdditionDeletion();
        diffTable();
    }

    public List<TableModel> getTableAdditions() {

        return tableAdditions;
    }

    public List<TableModel> getTableDeletions() {

        return tableDeletions;
    }
    public List<TableDiff> getTableDiff() {

        return tableDiffs;
    }

    private void diffTableAdditionDeletion() {

        Set<TableModel> sourceTableData = new HashSet<TableModel>(source.getTableData());
        Set<TableModel> targetTableData = new HashSet<TableModel>(target.getTableData());

        // Identify deleted tables
        Set<TableModel> deletedTables = new HashSet<TableModel>(sourceTableData);
        deletedTables.removeAll(targetTableData);
        tableDeletions.addAll(deletedTables);

        // Identify added tables
        Set<TableModel> addedTables = new HashSet<TableModel>(targetTableData);
        addedTables.removeAll(sourceTableData);
        tableAdditions.addAll(addedTables);
    }

    private void diffTable() {

        HashMap<String, TableModel> sourceTablesMap = new HashMap<String,TableModel>();
        for (TableModel table : source.getTableData()) {
            sourceTablesMap.put(table.getSchema() + "." + table.getName().getReference(), table);
        }

        Set<TableModel> existingTables = new HashSet<TableModel>(target.getTableData());
        existingTables.removeAll(getTableAdditions());

        for (TableModel table : existingTables) {
            TableDiff tableDiff = new TableDiff(table);
            tableDiffs.add(tableDiff);

            TableModel sourceTable = sourceTablesMap.get(table.getSchema() + "." + table.getName().getReference());

            Set<ColumnModel> sourceTableData = new HashSet<ColumnModel>(sourceTable.getColumns());
            Set<ColumnModel> targetTableData = new HashSet<ColumnModel>(table.getColumns());

            // Identify deleted columns
            Set<ColumnModel> deletedColumns = new HashSet<ColumnModel>(sourceTableData);
            deletedColumns.removeAll(targetTableData);

            tableDiff.getDeletedColumns().addAll(deletedColumns);

            // Identify added columns
            Set<ColumnModel> addedColumns = new HashSet<ColumnModel>(targetTableData);
            addedColumns.removeAll(sourceTableData);
            tableDiff.getAddedColumns().addAll(addedColumns);
        }
    }
}
