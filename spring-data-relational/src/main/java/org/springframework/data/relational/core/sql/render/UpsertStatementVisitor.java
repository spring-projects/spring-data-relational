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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jspecify.annotations.Nullable;
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
 * {@link UpsertRenderContext#renderer()}.
 *
 * @author Christoph Strobl
 * @since 4.x
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

			// TODO: this is highly inefficient - maybe we should just work with sqlidentifiers
			Map<Column, CharSequence> insertColumns = new LinkedHashMap<>();
			for (var assignment : source.getAssignments()) {
				if (assignment instanceof AssignValue av) {
					insertColumns.put(av.getColumn(), getBinding(av.getValue(), context));
				}
			}

			Map<SqlIdentifier, CharSequence> bindings = new HashMap<>(insertColumns.size());
			for (Entry<Column, CharSequence> e : insertColumns.entrySet()) {
				if (bindings.put(e.getKey().getName(), e.getValue()) != null) {
					throw new IllegalStateException("Duplicate key");
				}
			}

			UpsertRenderContext upsertContext = context.getUpsertRenderContext();
			UpsertRenderingContext renderingContext = UpsertRenderingContext.of(context, bindings::get);
			String sql = upsertContext.renderer().render(source.getTable(), new UpsertStatementRenderer.Columns(
					new ArrayList<>(insertColumns.keySet()), source.getConflictColumns(), bindings), renderingContext);
			builder.append(sql);

			return Delegation.leave();
		}

		return Delegation.retain();
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
}
