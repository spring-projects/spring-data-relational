/*
 * Copyright 2023-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.mapping.schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
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

	public static SchemaDiff diff(Tables mappedEntities, Tables existingTables, Comparator<String> nameComparator) {

		Map<String, Table> existingIndex = createMapping(existingTables.tables(), SchemaDiff::getKey, nameComparator);
		Map<String, Table> mappedIndex = createMapping(mappedEntities.tables(), SchemaDiff::getKey, nameComparator);

		List<Table> toCreate = getTablesToCreate(mappedEntities, withTableKey(existingIndex::containsKey));
		List<Table> toDrop = getTablesToDrop(existingTables, withTableKey(mappedIndex::containsKey));

		List<TableDiff> tableDiffs = diffTable(mappedEntities, existingIndex, withTableKey(existingIndex::containsKey),
				nameComparator);

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

	private static List<TableDiff> diffTable(Tables mappedEntities, Map<String, Table> existingIndex,
			Predicate<Table> includeTable, Comparator<String> nameComparator) {

		List<TableDiff> tableDiffs = new ArrayList<>();

		for (Table mappedEntity : mappedEntities.tables()) {

			if (!includeTable.test(mappedEntity)) {
				continue;
			}

			// TODO: How to handle changed columns (type?)

			Table existingTable = existingIndex.get(getKey(mappedEntity));
			TableDiff tableDiff = new TableDiff(mappedEntity);

			Map<String, Column> mappedColumns = createMapping(mappedEntity.columns(), Column::name, nameComparator);
			mappedEntity.keyColumns().forEach(it -> mappedColumns.put(it.name(), it));

			Map<String, Column> existingColumns = createMapping(existingTable.columns(), Column::name, nameComparator);
			existingTable.keyColumns().forEach(it -> existingColumns.put(it.name(), it));

			// Identify deleted columns
			Map<String, Column> toDelete = new TreeMap<>(nameComparator);
			toDelete.putAll(existingColumns);
			mappedColumns.keySet().forEach(toDelete::remove);

			tableDiff.columnsToDrop().addAll(toDelete.values());

			// Identify added columns
			Map<String, Column> addedColumns = new TreeMap<>(nameComparator);
			addedColumns.putAll(mappedColumns);

			existingColumns.keySet().forEach(addedColumns::remove);

			// Add columns in order. This order can interleave with existing columns.
			for (Column column : mappedEntity.keyColumns()) {
				if (addedColumns.containsKey(column.name())) {
					tableDiff.columnsToAdd().add(column);
				}
			}

			for (Column column : mappedEntity.columns()) {
				if (addedColumns.containsKey(column.name())) {
					tableDiff.columnsToAdd().add(column);
				}
			}

			tableDiffs.add(tableDiff);
		}

		return tableDiffs;
	}

	private static <T> SortedMap<String, T> createMapping(List<T> items, Function<T, String> keyFunction,
			Comparator<String> nameComparator) {

		SortedMap<String, T> mapping = new TreeMap<>(nameComparator);
		items.forEach(it -> mapping.put(keyFunction.apply(it), it));
		return mapping;
	}

	private static String getKey(Table table) {
		return table.schema() + "." + table.name();
	}

	private static Predicate<Table> withTableKey(Predicate<String> predicate) {
		return it -> predicate.test(getKey(it));
	}

}
