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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.data.relational.core.DialectCapable;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Upsert;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.render.UpsertStatementRenderer.UpsertRenderingContext;
import org.springframework.util.Assert;

/**
 * {@link PartRenderer} for {@link Upsert} statements. Uses the {@link Upsert} interface accessors directly to obtain
 * the insert assignments and conflict columns, then delegates dialect-specific rendering to
 * {@link UpsertStatementRenderers#forDialect(Dialect)}.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
public class UpsertStatementVisitor extends DelegatingVisitor implements PartRenderer {

	private final StringBuilder builder = new StringBuilder();
	private final RenderContext context;

	public UpsertStatementVisitor(RenderContext context) {

		Assert.notNull(context, "RenderContext must not be null");
		this.context = context;
	}

	@Override
	@Nullable
	Delegation doEnter(Visitable segment) {
		return Delegation.retain();
	}

	@Override
	Delegation doLeave(Visitable segment) {

		if (segment instanceof Upsert source) {

			Assert.isTrue(context.getUpsertRenderContext().supportsUpsert(), "Upsert is not supported by dialect");

			InsertColumnsAndBindings columnsAndBindings = resolveInsertColumnsAndBindings(source);

			Dialect dialect = context instanceof DialectCapable dialectCapable ? dialectCapable.getDialect()
					: AnsiDialect.INSTANCE;

			UpsertStatementRenderer statementRenderer = UpsertStatementRenderers.forDialect(dialect);
			UpsertRenderingContext renderingContext = UpsertRenderingContext.of(context, columnsAndBindings.bindings()::get);

			String sql = statementRenderer.render(source.getTable(), new UpsertStatementRenderer.Columns(
					columnsAndBindings.insertColumns(), source.getConflictColumns(), columnsAndBindings.bindings()), renderingContext);
			builder.append(sql);

			return Delegation.leave();
		}

		return Delegation.retain();
	}

	private InsertColumnsAndBindings resolveInsertColumnsAndBindings(Upsert source) {

		var assignments = source.getAssignments();
		List<Column> insertColumns = new ArrayList<>(assignments.size());
		Map<SqlIdentifier, CharSequence> bindings = new HashMap<>(assignments.size());

		for (var assignment : assignments) {
			if (assignment instanceof AssignValue av) {
				Column column = av.getColumn();
				insertColumns.add(column);
				CharSequence binding = getBinding(av.getValue(), context);
				if (bindings.put(column.getName(), binding) != null) {
					throw new IllegalStateException("Duplicate key %s".formatted(column.getName()));
				}
			}
		}

		return new InsertColumnsAndBindings(insertColumns, bindings);
	}

	CharSequence getBinding(Expression expression, RenderContext context) {

		ExpressionVisitor expressionVisitor = new ExpressionVisitor(context);
		expression.visit(expressionVisitor);
		return expressionVisitor.getRenderedPart();
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}

	private record InsertColumnsAndBindings(List<Column> insertColumns, Map<SqlIdentifier, CharSequence> bindings) {
	}
}
