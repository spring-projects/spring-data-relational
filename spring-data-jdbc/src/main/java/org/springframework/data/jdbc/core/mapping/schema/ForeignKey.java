package org.springframework.data.jdbc.core.mapping.schema;

import java.util.Objects;

/**
 * Models a Foreign Key for generating SQL for Schema generation.
 *
 * @author Evgenii Koba
 * @since 3.2
 */
record ForeignKey(String name, String tableName, String columnName, String referencedTableName,
									String referencedColumnName) {
	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		ForeignKey that = (ForeignKey) o;
		return Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}
}
