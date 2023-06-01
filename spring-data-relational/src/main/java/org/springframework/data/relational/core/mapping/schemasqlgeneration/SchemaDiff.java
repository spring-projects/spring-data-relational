package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is created to return the difference between a source and target {@link MappedTables} The difference
 * consists of Table Additions, Deletions, and Modified Tables (i.e. table exists in both source and target - but has
 * columns to add or delete)
 *
 * @author Kurt Niemi
 * @since 3.2
 */
class SchemaDiff {

	private final MappedTables source;
	private final MappedTables target;

	private final List<TableModel> tableAdditions = new ArrayList<>();
	private final List<TableModel> tableDeletions = new ArrayList<>();
	private final List<TableDiff> tableDiffs = new ArrayList<>();

	/**
	 * Compare two {@link MappedTables} to identify differences.
	 *
	 * @param target model reflecting current database state.
	 * @param source model reflecting desired database state.
	 */
	public SchemaDiff(MappedTables target, MappedTables source) {

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

		List<TableModel> sourceTableData = new ArrayList<>(source.getTableData());
		List<TableModel> targetTableData = new ArrayList<>(target.getTableData());

		// Identify deleted tables
		List<TableModel> deletedTables = new ArrayList<>(sourceTableData);
		deletedTables.removeAll(targetTableData);
		tableDeletions.addAll(deletedTables);

		// Identify added tables
		List<TableModel> addedTables = new ArrayList<>(targetTableData);
		addedTables.removeAll(sourceTableData);
		tableAdditions.addAll(addedTables);
	}

	private void diffTable() {

		Map<String, TableModel> sourceTablesMap = new LinkedHashMap<>();
		for (TableModel table : source.getTableData()) {
			sourceTablesMap.put(table.schema() + "." + table.name(), table);
		}

		Set<TableModel> existingTables = new LinkedHashSet<>(target.getTableData());
		getTableAdditions().forEach(existingTables::remove);

		for (TableModel table : existingTables) {
			TableDiff tableDiff = new TableDiff(table);
			tableDiffs.add(tableDiff);

			TableModel sourceTable = sourceTablesMap.get(table.schema() + "." + table.name());

			Set<ColumnModel> sourceTableData = new LinkedHashSet<>(sourceTable.columns());
			Set<ColumnModel> targetTableData = new LinkedHashSet<>(table.columns());

			// Identify deleted columns
			Set<ColumnModel> deletedColumns = new LinkedHashSet<>(sourceTableData);
			deletedColumns.removeAll(targetTableData);

			tableDiff.columnsToDrop().addAll(deletedColumns);

			// Identify added columns
			Set<ColumnModel> addedColumns = new LinkedHashSet<>(targetTableData);
			addedColumns.removeAll(sourceTableData);
			tableDiff.columnsToAdd().addAll(addedColumns);
		}
	}
}
