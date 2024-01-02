/*
 * Copyright 2020-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.Named;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.TableLike;

/**
 * Utility to render {@link Column} and {@link Table} names using {@link SqlIdentifier} and {@link RenderContext} to
 * SQL.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class NameRenderer {

	/**
	 * Render the {@link TableLike#getName() table name } with considering the
	 * {@link RenderNamingStrategy#getName(TableLike) naming strategy}.
	 */
	static CharSequence render(RenderContext context, TableLike table) {
		return render(context, context.getNamingStrategy().getName(table));
	}

	/**
	 * Render the {@link Column#getName() column name} with considering the {@link RenderNamingStrategy#getName(Column)
	 * naming strategy}.
	 */
	static CharSequence render(RenderContext context, Column column) {
		return render(context, context.getNamingStrategy().getName(column));
	}

	/**
	 * Render the {@link Named#getName() name}.
	 */
	static CharSequence render(RenderContext context, Named named) {
		return render(context, named.getName());
	}

	/**
	 * Render the {@link Aliased#getAlias() alias}.
	 */
	static CharSequence render(RenderContext context, Aliased aliased) {
		return render(context, aliased.getAlias());
	}

	/**
	 * Render the {@link Table#getReferenceName()} table reference name} with considering the
	 * {@link RenderNamingStrategy#getReferenceName(TableLike) naming strategy}.
	 */
	static CharSequence reference(RenderContext context, TableLike table) {
		return render(context, context.getNamingStrategy().getReferenceName(table));
	}

	/**
	 * Render the {@link Column#getReferenceName()} column reference name} with considering the
	 * {@link RenderNamingStrategy#getReferenceName(Column) naming strategy}.
	 */
	static CharSequence reference(RenderContext context, Column column) {
		return render(context, context.getNamingStrategy().getReferenceName(column));
	}

	/**
	 * Render the fully-qualified table and column name with considering the naming strategies of each component.
	 *
	 * @see RenderNamingStrategy#getReferenceName
	 */
	static CharSequence fullyQualifiedReference(RenderContext context, Column column) {

		RenderNamingStrategy namingStrategy = context.getNamingStrategy();

		if (column instanceof Aliased) {
			return render(context, namingStrategy.getReferenceName(column));
		}

		return render(context, SqlIdentifier.from(namingStrategy.getReferenceName(column.getTable()),
				namingStrategy.getReferenceName(column)));
	}

	/**
	 * Render the fully-qualified table and column name with considering the naming strategies of each component without
	 * using the alias for the column. For the table the alias is still used.
	 *
	 * @see #fullyQualifiedReference(RenderContext, Column)
	 * @since 2.3
	 */
	static CharSequence fullyQualifiedUnaliasedReference(RenderContext context, Column column) {

		RenderNamingStrategy namingStrategy = context.getNamingStrategy();

		return render(context,
				SqlIdentifier.from(namingStrategy.getReferenceName(column.getTable()), namingStrategy.getName(column)));
	}

	/**
	 * Render the {@link SqlIdentifier#toSql(IdentifierProcessing) identifier to SQL} considering
	 * {@link IdentifierProcessing}.
	 */
	static CharSequence render(RenderContext context, SqlIdentifier identifier) {
		return identifier.toSql(context.getIdentifierProcessing());
	}

	private NameRenderer() {}
}
