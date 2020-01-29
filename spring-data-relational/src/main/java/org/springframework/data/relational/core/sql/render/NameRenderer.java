/*
 * Copyright 2020 the original author or authors.
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

/**
 * Utility to render {@link Column} and {@link Table} names using {@link SqlIdentifier} and {@link RenderContext} to
 * SQL.
 *
 * @author Mark Paluch
 */
class NameRenderer {

	/**
	 * Render the {@link Table#getName() table name } with considering the {@link RenderNamingStrategy#getName(Table)
	 * naming strategy}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence render(RenderContext context, Table table) {
		return render(context, context.getNamingStrategy().getName(table));
	}

	/**
	 * Render the {@link Column#getName() column name} with considering the {@link RenderNamingStrategy#getName(Column)
	 * naming strategy}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence render(RenderContext context, Column column) {
		return render(context, context.getNamingStrategy().getName(column));
	}

	/**
	 * Render the {@link Named#getName() name}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence render(RenderContext context, Named named) {
		return render(context, named.getName());
	}

	/**
	 * Render the {@link Aliased#getAlias() alias}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence render(RenderContext context, Aliased aliased) {
		return render(context, aliased.getAlias());
	}

	/**
	 * Render the {@link Table#getReferenceName()} table reference name} with considering the
	 * {@link RenderNamingStrategy#getReferenceName(Table) naming strategy}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence reference(RenderContext context, Table table) {
		return render(context, context.getNamingStrategy().getReferenceName(table));
	}

	/**
	 * Render the {@link Column#getReferenceName()} column reference name} with considering the
	 * {@link RenderNamingStrategy#getReferenceName(Column) naming strategy}.
	 *
	 * @param context
	 * @param table
	 * @return
	 */
	static CharSequence reference(RenderContext context, Column column) {
		return render(context, context.getNamingStrategy().getReferenceName(column));
	}

	/**
	 * Render the fully-qualified table and column name with considering the naming strategies of each component.
	 *
	 * @param context
	 * @param column
	 * @return
	 * @see RenderNamingStrategy#getReferenceName
	 */
	static CharSequence fullyQualifiedReference(RenderContext context, Column column) {

		RenderNamingStrategy namingStrategy = context.getNamingStrategy();

		return render(context, SqlIdentifier.from(namingStrategy.getReferenceName(column.getTable()),
				namingStrategy.getReferenceName(column)));
	}

	/**
	 * Render the {@link SqlIdentifier#toSql(IdentifierProcessing) identifier to SQL} considering
	 * {@link IdentifierProcessing}.
	 *
	 * @param context
	 * @param identifier
	 * @return
	 */
	static CharSequence render(RenderContext context, SqlIdentifier identifier) {
		return identifier.toSql(context.getIdentifierProcessing());
	}

	private NameRenderer() {}
}
