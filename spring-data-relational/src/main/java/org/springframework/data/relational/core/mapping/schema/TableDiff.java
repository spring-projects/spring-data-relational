package org.springframework.data.relational.core.mapping.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to keep track of columns that should be added or deleted, when performing a difference between a source and
 * target {@link Tables}.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
record TableDiff(Table table, List<Column> columnsToAdd, List<Column> columnsToDrop) {

	public TableDiff(Table table) {
		this(table, new ArrayList<>(), new ArrayList<>());
	}

}
