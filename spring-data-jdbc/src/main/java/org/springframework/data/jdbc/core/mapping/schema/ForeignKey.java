package org.springframework.data.jdbc.core.mapping.schema;

import java.util.List;
import java.util.Objects;

/**
 * Models a Foreign Key for generating SQL for Schema generation.
 *
 * @author Evgenii Koba
 * @since 3.3
 */
record ForeignKey(String name, String tableName, List<String> columnNames, String referencedTableName,
		List<String> referencedColumnNames) {
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ForeignKey that = (ForeignKey) o;
		return Objects.equals(tableName, that.tableName) && Objects.equals(columnNames, that.columnNames)
				&& Objects.equals(referencedTableName, that.referencedTableName)
				&& Objects.equals(referencedColumnNames, that.referencedColumnNames);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableName, columnNames, referencedTableName, referencedColumnNames);
	}
}
