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
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;

import org.jspecify.annotations.Nullable;
import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;

/**
 * Dialect-specific upsert SQL as a single statement string (e.g. {@code MERGE}, {@code INSERT ... ON CONFLICT},
 * {@code INSERT ... ON DUPLICATE KEY UPDATE}). Callers resolve {@link Column}s and {@link Table}; implementations only
 * assemble syntax and use {@link UpsertRenderingContext} so names and bind markers match the enclosing
 * {@link RenderContext}. Concrete renderers are defined in {@link UpsertStatementRenderers}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
interface UpsertStatementRenderer {

	/**
	 * Render the full upsert statement for {@code table}.
	 *
	 * @param table target table
	 * @param columns {@link Columns#insertColumns()} values to insert; {@link Columns#conflictColumns()} keys that
	 *          identify an existing row for the dialect's conflict/merge semantics
	 * @param ctx rendering hooks (quoting, bind markers) tied to the current {@link RenderContext}
	 * @return executable upsert SQL text (parameter placeholders as produced by {@code ctx})
	 */
	String render(Table table, Columns columns, UpsertRenderingContext ctx);

	/**
	 * Building blocks for {@link UpsertStatementRenderer}.
	 */
	interface UpsertRenderingContext {

		/**
		 * Backs upsert rendering with {@code renderContext} (quoting, bind marker style).
		 *
		 * @param renderContext active SQL render context
		 * @return context passed to {@link UpsertStatementRenderer#render}
		 */
		static UpsertRenderingContext of(RenderContext renderContext,
				Function<SqlIdentifier, CharSequence> bindMarkerResolution) {

			return new UpsertRenderingContext() {

				@Override
				public RenderContext renderContext() {
					return renderContext;
				}

				@Override
				public CharSequence bindMarker(Column column,
						BiFunction<CharSequence, CharSequence, CharSequence> bindMarkerFn) {

					CharSequence maker = bindMarkerResolution.apply(column.getName());
					return bindMarkerFn.apply(columnName(column), maker);
				}
			};
		}

		/** @return render context */
		RenderContext renderContext();

		/** @return rendered table reference */
		default CharSequence tableName(Table table) {
			return NameRenderer.render(renderContext(), table);
		}

		/** @return rendered column reference without a table qualifier */
		default CharSequence columnName(Column column) {
			return columnName(SqlIdentifier.EMPTY, column);
		}

		/** @return {@code column} rendered with {@link Aliased#getAlias()} as qualifier */
		default CharSequence columnName(Aliased table, Column column) {
			return columnName(table.getAlias(), column);
		}

		/**
		 * @param tableAlias table or empty; if empty, column only, else {@code alias.column}
		 * @return rendered column reference
		 */
		default CharSequence columnName(SqlIdentifier tableAlias, Column column) {
			if (tableAlias.equals(SqlIdentifier.EMPTY)) {
				return NameRenderer.render(renderContext(), column);
			}
			return "%s.%s".formatted(NameRenderer.render(renderContext(), tableAlias),
					NameRenderer.render(renderContext(), column));
		}

		/** @return each column name rendered (unqualified) and collected (e.g. comma-separated) */
		default CharSequence columnNames(List<Column> columns,
				Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return columnNames(SqlIdentifier.EMPTY, columns, collector);
		}

		/** @return like {@link #columnNames(List, Collector)} but with {@code tableAlias} on each column */
		default CharSequence columnNames(SqlIdentifier tableAlias, List<Column> columns,
				Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return columns.stream().map(column -> columnName(tableAlias, column)).collect(collector);
		}

		/** @return {@code :reference} bind marker from {@link Column#getName()} */
		default CharSequence bindMarker(Column column) {
			return bindMarker(column, (columnName, bindMarker) -> bindMarker);
		}

		/**
		 * @param bindMarkerFn receives rendered column name and default {@code :reference} marker; returns fragment to
		 *          embed
		 * @return result of {@code bindMarkerFn}
		 */
		default CharSequence bindMarker(Column column, BiFunction<CharSequence, CharSequence, CharSequence> bindMarkerFn) {
			return bindMarkerFn.apply(columnName(column), ":%s".formatted(column.getName().getReference()));
		}

		/** @return bind marker per column, collected */
		default CharSequence bindMarkers(List<Column> columns,
				Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return columns.stream().map(column -> bindMarker(column, (columnName, bindMarker) -> bindMarker))
					.collect(collector);
		}

		/** @return bind markers using {@code bindMarkerFn} per column, collected */
		default CharSequence bindMarkers(List<Column> columns,
				BiFunction<CharSequence, CharSequence, CharSequence> bindMarkerFn,
				Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return columns.stream().map(column -> bindMarker(column, bindMarkerFn)).collect(collector);
		}

		/** @return {@code targetColumn = sourceColumn} for the given aliases */
		default CharSequence assignment(SqlIdentifier targetTableAlias, Column column, SqlIdentifier sourceTableAlias) {
			return assignment(targetTableAlias, column, sourceTableAlias, Function.identity());
		}

		/**
		 * @param sourceValueFn transforms the rendered source column reference (e.g. wrap in a function call)
		 * @return {@code targetColumn =} {@code sourceValueFn(sourceColumn)}
		 */
		default CharSequence assignment(SqlIdentifier targetTableAlias, Column column, SqlIdentifier sourceTableAlias,
				Function<CharSequence, CharSequence> sourceValueFn) {

			CharSequence targetColumn = columnName(targetTableAlias, column);
			CharSequence sourceColumn = columnName(sourceTableAlias, column);
			return "%s = %s".formatted(targetColumn, sourceValueFn.apply(sourceColumn));
		}

		/** @return one assignment per column, collected */
		default CharSequence assignments(SqlIdentifier targetTableAlias, List<Column> columns,
				SqlIdentifier sourceTableAlias, Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return assignments(targetTableAlias, columns, sourceTableAlias, Function.identity(), collector);
		}

		/** @return assignments with {@code sourceValueFn} applied to each source side, collected */
		default CharSequence assignments(SqlIdentifier targetTableAlias, List<Column> columns,
				SqlIdentifier sourceTableAlias, Function<CharSequence, CharSequence> sourceValueFn,
				Collector<CharSequence, ?, ? extends CharSequence> collector) {
			return columns.stream().map(column -> assignment(targetTableAlias, column, sourceTableAlias, sourceValueFn))
					.collect(collector);
		}
	}

	class Columns {

		private final Map<SqlIdentifier, CharSequence> bindings;
		private final List<Column> insertColumns;
		private final List<Column> conflictColumns;
		private final List<Column> updateColumns;

		public Columns(List<Column> insertColumns, List<Column> conflictColumns,
				Map<SqlIdentifier, CharSequence> bindings) {

			this.bindings = bindings;
			this.insertColumns = insertColumns;
			this.conflictColumns = conflictColumns;
			this.updateColumns = insertColumns.stream()
					.filter(col -> conflictColumns.stream().noneMatch(it -> it.getName().equals(col.getName()))).toList();
		}

		/**
		 * Columns to assign on update.
		 */
		List<Column> updateColumns() {
			return updateColumns;
		}

		/**
		 * Columns insert.
		 */
		public List<Column> insertColumns() {
			return insertColumns;
		}

		/**
		 * Columns defining the conflict condition.
		 */
		public List<Column> conflictColumns() {
			return conflictColumns;
		}

		public @Nullable CharSequence binding(Column column) {
			return bindings.get(column.getName());
		}
	}
}
