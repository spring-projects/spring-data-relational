/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.util.Assert;

/**
 * Concrete {@link UpsertStatementRenderer} implementations.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
class UpsertStatementRenderers {

	/** Target table alias in {@code MERGE} statements. */
	static final SqlIdentifier MERGE_TARGET_TABLE_ALIAS = SqlIdentifier.quoted("_t");

	/** Source (values) alias in {@code MERGE} statements. */
	static final SqlIdentifier MERGE_SOURCE_TABLE_ALIAS = SqlIdentifier.quoted("_s");

	/** Return the {@link UpsertStatementRenderer} for the given {@link Dialect}. */
	static UpsertStatementRenderer from(Dialect dialect) {

		String dialectName = dialect.getName().toLowerCase(Locale.ROOT).replace("dialect", "");
		return switch (dialectName) {
			case "mysql", "mariadb" -> mySql();
			case "oracle" -> oracle();
			case "postgres" -> postgres();
			case "sqlserver" -> sqlServer();
			default -> merge();
		};
	}

	/**
	 * {@code MERGE} upsert statement renderer.
	 */
	static UpsertStatementRenderer merge() {
		return Merge.INSTANCE;
	}

	/**
	 * MySQL-specific renderer using {@code INSERT ... ON DUPLICATE KEY UPDATE}.
	 */
	static UpsertStatementRenderer mySql() {
		return MySql.INSTANCE;
	}

	/**
	 * Oracle-specific renderer using {@code MERGE} with {@code SELECT ... FROM DUAL} as source.
	 */
	static UpsertStatementRenderer oracle() {
		return Oracle.INSTANCE;
	}

	/**
	 * PostgreSQL-specific renderer using {@code INSERT ... ON CONFLICT ... DO UPDATE SET} / {@code DO NOTHING}.
	 */
	static UpsertStatementRenderer postgres() {
		return Postgres.INSTANCE;
	}

	/**
	 * SQL Server-specific renderer using {@code MERGE} with a trailing semicolon (batch separator).
	 */
	static UpsertStatementRenderer sqlServer() {
		return SqlServer.INSTANCE;
	}

	private UpsertStatementRenderers() {}

	/**
	 * Standard SQL {@code MERGE} using a table value constructor {@code (VALUES (?, ?)) AS s (col1, col2)} (H2, HSQLDB,
	 * DB2, etc.).
	 */
	static class Merge implements UpsertStatementRenderer {

		static final Merge INSTANCE = new Merge();

		@Override
		public String render(Table table, Columns columns, UpsertRenderingContext ctx) {

			Assert.notEmpty(columns.insertColumns(), "Insert columns must not be empty");
			Assert.notEmpty(columns.conflictColumns(), "Conflict columns must not be empty");

			CharSequence tableName = ctx.tableName(table);
			CharSequence insertColumnNames = ctx.columnNames(columns.insertColumns(), Collectors.joining(", "));
			CharSequence bindMarkers = ctx.bindMarkers(columns.insertColumns(), Collectors.joining(", "));
			CharSequence onCondition = ctx.assignments(MERGE_TARGET_TABLE_ALIAS, columns.conflictColumns(),
					MERGE_SOURCE_TABLE_ALIAS, Collectors.joining(" AND "));
			CharSequence insertValuesSql = ctx.columnNames(MERGE_SOURCE_TABLE_ALIAS, columns.insertColumns(),
					Collectors.joining(", "));

			String insertClause = "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)".formatted(insertColumnNames,
					insertValuesSql);

			List<Column> updateColumns = columns.updateColumns();
			if (updateColumns.isEmpty()) {
				return "MERGE INTO %s %s USING (VALUES (%s)) AS %s (%s) ON %s %s".formatted( //
						tableName, //
						MERGE_TARGET_TABLE_ALIAS, //
						bindMarkers, //
						MERGE_SOURCE_TABLE_ALIAS, //
						insertColumnNames, //
						onCondition, //
						insertClause);
			}

			CharSequence updateSetClause = ctx.assignments(MERGE_TARGET_TABLE_ALIAS, columns.updateColumns(),
					MERGE_SOURCE_TABLE_ALIAS, Collectors.joining(", "));

			return "MERGE INTO %s %s USING (VALUES (%s)) AS %s (%s) ON %s WHEN MATCHED THEN UPDATE SET %s %s".formatted( //
					tableName, //
					MERGE_TARGET_TABLE_ALIAS, //
					bindMarkers, //
					MERGE_SOURCE_TABLE_ALIAS, //
					insertColumnNames, //
					onCondition, //
					updateSetClause, //
					insertClause);
		}

	}

	/**
	 * PostgreSQL {@code INSERT ... ON CONFLICT ... DO UPDATE SET} / {@code DO NOTHING}.
	 */
	static class Postgres implements UpsertStatementRenderer {

		static final Postgres INSTANCE = new Postgres();

		@Override
		public String render(Table table, Columns columns, UpsertRenderingContext ctx) {

			Assert.notEmpty(columns.insertColumns(), "Insert columns must not be empty");
			Assert.notEmpty(columns.conflictColumns(), "Conflict columns must not be empty");

			CharSequence tableName = ctx.tableName(table);
			CharSequence insertColumnNames = ctx.columnNames(columns.insertColumns(), Collectors.joining(", "));
			CharSequence conflictColumnNames = ctx.columnNames(columns.conflictColumns(), Collectors.joining(", "));
			CharSequence bindMarkers = ctx.bindMarkers(columns.insertColumns(), Collectors.joining(", "));

			if (columns.updateColumns().isEmpty()) {
				return "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING".formatted(//
						tableName, //
						insertColumnNames, //
						bindMarkers, //
						conflictColumnNames);
			}

			CharSequence setValues = ctx.assignments(SqlIdentifier.EMPTY, columns.updateColumns(), SqlIdentifier.EMPTY,
					"EXCLUDED.%s"::formatted, Collectors.joining(", "));

			return "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s".formatted(//
					tableName, insertColumnNames, bindMarkers, conflictColumnNames, setValues);
		}

	}

	/**
	 * MySQL / MariaDB {@code INSERT ... ON DUPLICATE KEY UPDATE}.
	 */
	static class MySql implements UpsertStatementRenderer {

		static final MySql INSTANCE = new MySql();

		@Override
		public String render(Table table, Columns columns, UpsertRenderingContext ctx) {

			Assert.notEmpty(columns.insertColumns(), "Insert columns must not be empty");
			Assert.notEmpty(columns.conflictColumns(), "Conflict columns must not be empty");

			CharSequence tableName = ctx.tableName(table);
			CharSequence columnNames = ctx.columnNames(columns.insertColumns(), Collectors.joining(", "));
			CharSequence bindMarkers = ctx.bindMarkers(columns.insertColumns(), Collectors.joining(", "));

			List<Column> updateColumns = columnsToUpdate(columns);

			CharSequence setValues = ctx.assignments(SqlIdentifier.EMPTY, updateColumns, SqlIdentifier.EMPTY,
					"VALUES(%s)"::formatted, Collectors.joining(", "));

			return "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s".formatted( //
					tableName, //
					columnNames, //
					bindMarkers, //
					setValues);
		}

		private static List<Column> columnsToUpdate(Columns columns) {

			if (!columns.updateColumns().isEmpty()) {
				return columns.updateColumns();
			}

			/* MySQL requires at least one column to be updated.
			 * and since all columns are conflicting we can pick any
			 *
			 * Note to future self: We cannot use INSERT IGNORE here, as it would suppress data validation errors.
			 */

			return columns.conflictColumns().subList(0, 1);
		}
	}

	/**
	 * Oracle {@code MERGE} with {@code SELECT ... FROM DUAL} as source.
	 */
	static class Oracle implements UpsertStatementRenderer {

		static final Oracle INSTANCE = new Oracle();

		@Override
		public String render(Table table, Columns columns, UpsertRenderingContext ctx) {

			Assert.notEmpty(columns.insertColumns(), "Insert columns must not be empty");
			Assert.notEmpty(columns.conflictColumns(), "Conflict columns must not be empty");

			CharSequence tableName = ctx.tableName(table);
			CharSequence insertColumnNames = ctx.columnNames(columns.insertColumns(), Collectors.joining(", "));
			CharSequence sourceSelectList = ctx.bindMarkers(columns.insertColumns(),
					(columnName, bindMarker) -> "%s AS %s".formatted(bindMarker, columnName), Collectors.joining(", "));
			CharSequence onCondition = ctx.assignments(MERGE_TARGET_TABLE_ALIAS, columns.conflictColumns(),
					MERGE_SOURCE_TABLE_ALIAS, Collectors.joining(" AND "));
			CharSequence insertValuesSql = ctx.columnNames(MERGE_SOURCE_TABLE_ALIAS, columns.insertColumns(),
					Collectors.joining(", "));

			String insertClause = "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)".formatted(insertColumnNames,
					insertValuesSql);

			List<Column> updateColumns = columns.updateColumns();
			if (updateColumns.isEmpty()) {
				return "MERGE INTO %s %s USING (SELECT %s FROM DUAL) %s ON (%s) %s".formatted( //
						tableName, //
						MERGE_TARGET_TABLE_ALIAS, //
						sourceSelectList, //
						MERGE_SOURCE_TABLE_ALIAS, //
						onCondition, //
						insertClause);
			}

			CharSequence updateSetClause = ctx.assignments(MERGE_TARGET_TABLE_ALIAS, columns.updateColumns(),
					MERGE_SOURCE_TABLE_ALIAS, Collectors.joining(", "));

			return "MERGE INTO %s %s USING (SELECT %s FROM DUAL) %s ON (%s) WHEN MATCHED THEN UPDATE SET %s %s".formatted( //
					tableName, //
					MERGE_TARGET_TABLE_ALIAS, //
					sourceSelectList, //
					MERGE_SOURCE_TABLE_ALIAS, //
					onCondition, //
					updateSetClause, //
					insertClause);
		}

	}

	/**
	 * SQL Server {@code MERGE}: same body as {@link Merge} with a trailing semicolon (batch separator).
	 */
	static class SqlServer extends Merge {

		private static final String STATEMENT_TERMINATOR = ";";

		static final SqlServer INSTANCE = new SqlServer();

		@Override
		public String render(Table table, Columns columns, UpsertRenderingContext ctx) {
			return super.render(table, columns, ctx) + STATEMENT_TERMINATOR;
		}

	}

}
