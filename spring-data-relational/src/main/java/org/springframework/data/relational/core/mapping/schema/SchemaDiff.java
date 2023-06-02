package org.springframework.data.relational.core.mapping.schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This class is created to return the difference between a source and target {@link Tables} The difference consists of
 * Table Additions, Deletions, and Modified Tables (i.e. table exists in both source and target - but has columns to add
 * or delete)
 *
 * @author Kurt Niemi
 * @since 3.2
 */
record SchemaDiff(List<Table> tableAdditions, List<Table> tableDeletions, List<TableDiff> tableDiffs) {

	public static SchemaDiff diff(Tables mappedEntities, Tables existingTables) {

		Set<Table> existingIndex = new HashSet<>(existingTables.tables());
		Set<Table> mappedIndex = new HashSet<>(mappedEntities.tables());

		List<Table> toCreate = getTablesToCreate(mappedEntities, existingIndex::contains);
		List<Table> toDrop = getTablesToDrop(existingTables, mappedIndex::contains);

		List<TableDiff> tableDiffs = diffTable(mappedEntities, existingTables, existingIndex::contains);

		return new SchemaDiff(toCreate, toDrop, tableDiffs);
	}

	private static List<Table> getTablesToCreate(Tables mappedEntities, Predicate<Table> excludeTable) {

		List<Table> toCreate = new ArrayList<>(mappedEntities.tables().size());

		for (Table table : mappedEntities.tables()) {
			if (!excludeTable.test(table)) {
				toCreate.add(table);
			}
		}

		return toCreate;
	}

	private static List<Table> getTablesToDrop(Tables existingTables, Predicate<Table> excludeTable) {

		List<Table> toDrop = new ArrayList<>(existingTables.tables().size());

		for (Table table : existingTables.tables()) {
			if (!excludeTable.test(table)) {
				toDrop.add(table);
			}
		}

		return toDrop;
	}

	private static List<TableDiff> diffTable(Tables mappedEntities, Tables existingTables,
			Predicate<Table> includeTable) {

		List<TableDiff> tableDiffs = new ArrayList<>();
		Map<Table, Table> existingIndex = new HashMap<>(existingTables.tables().size());
		existingTables.tables().forEach(it -> existingIndex.put(it, it));

		for (Table mappedEntity : mappedEntities.tables()) {

			if (!includeTable.test(mappedEntity)) {
				continue;
			}

			// TODO: How to handle changed columns (type?)

			Table existingTable = existingIndex.get(mappedEntity);
			TableDiff tableDiff = new TableDiff(mappedEntity);

			Set<Column> mappedColumns = new LinkedHashSet<>(mappedEntity.columns());
			Set<Column> existingColumns = new LinkedHashSet<>(existingTable.columns());

			// Identify deleted columns
			Set<Column> toDelete = new LinkedHashSet<>(existingColumns);
			toDelete.removeAll(mappedColumns);

			tableDiff.columnsToDrop().addAll(toDelete);

			// Identify added columns
			Set<Column> addedColumns = new LinkedHashSet<>(mappedColumns);
			addedColumns.removeAll(existingColumns);
			tableDiff.columnsToAdd().addAll(addedColumns);

			tableDiffs.add(tableDiff);
		}

		return tableDiffs;
	}

}
