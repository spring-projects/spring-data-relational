package org.springframework.data.jdbc.core.mapping.schema;

import java.util.List;

/**
 * Models a Foreign Key for generating SQL for Schema generation.
 *
 * @author Evgenii Koba
 * @since 3.2
 */
record ForeignKey(String name, String tableName, List<String> columnNames, String referencedTableName,
									List<String> referencedColumnNames) {
}
