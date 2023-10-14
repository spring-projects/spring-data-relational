package org.springframework.data.jdbc.core.mapping.schema;

/**
 * Models a Foreign Key for generating SQL for Schema generation.
 *
 * @author Evgenii Koba
 * @since 3.2
 */
record ForeignKey(String name, String tableName, String columnName, String referencedTableName,
									String referencedColumnName) {
}
