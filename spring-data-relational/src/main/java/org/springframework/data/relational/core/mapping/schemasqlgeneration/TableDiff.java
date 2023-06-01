package org.springframework.data.relational.core.mapping.schemasqlgeneration;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to keep track of columns that should be added or deleted, when performing a difference between a source and
 * target {@link MappedTables}.
 *
 * @author Kurt Niemi
 * @since 3.2
 */
record TableDiff(TableModel table, List<ColumnModel> columnsToAdd, List<ColumnModel> columnsToDrop) {

	public TableDiff(TableModel tableModel) {
		this(tableModel, new ArrayList<>(), new ArrayList<>());
	}

}
