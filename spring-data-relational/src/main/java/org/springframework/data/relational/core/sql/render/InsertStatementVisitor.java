/*
 * Copyright 2019-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Into;
import org.springframework.data.relational.core.sql.Values;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.util.Assert;

/**
 * {@link PartRenderer} for {@link Insert} statements.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 1.1
 */
class InsertStatementVisitor extends DelegatingVisitor implements PartRenderer {

	private final StringBuilder builder = new StringBuilder();
	private final StringBuilder into = new StringBuilder();
	private final StringBuilder columns = new StringBuilder();
	private final StringBuilder values = new StringBuilder();

	private final IntoClauseVisitor intoClauseVisitor;
	private final ColumnVisitor columnVisitor;
	private final ValuesVisitor valuesVisitor;
	private final RenderContext renderContext;

	InsertStatementVisitor(RenderContext renderContext) {

		Assert.notNull(renderContext, "renderContext must not be null");

		this.renderContext = renderContext;
		this.intoClauseVisitor = createIntoClauseVisitor(renderContext);
		this.columnVisitor = createColumnVisitor(renderContext);
		this.valuesVisitor = new ValuesVisitor(renderContext, values::append);
	}

	@Override
	public Delegation doEnter(Visitable segment) {

		if (segment instanceof Into) {
			return Delegation.delegateTo(this.intoClauseVisitor);
		}

		if (segment instanceof Column) {
			return Delegation.delegateTo(this.columnVisitor);
		}

		if (segment instanceof Values) {
			return Delegation.delegateTo(this.valuesVisitor);
		}

		return Delegation.retain();
	}

	@Override
	public Delegation doLeave(Visitable segment) {

		if (segment instanceof Insert) {

			builder.append("INSERT");

			builder.append(" INTO ").append(into);

			addInsertColumnsIfPresent();

			addInsertValuesIfPresentElseDefault();

			return Delegation.leave();
		}

		return Delegation.retain();
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}

	private void addInsertValuesIfPresentElseDefault() {

		if (values.length() != 0) {
			builder.append(" VALUES (").append(values).append(")");
		} else {
			addInsertWithDefaultValuesToBuilder();
		}
	}

	private void addInsertColumnsIfPresent() {

		if (columns.length() != 0) {
			builder.append(" (").append(columns).append(")");
		}
	}

	private void addInsertWithDefaultValuesToBuilder() {
		builder.append(renderContext.getInsertRenderContext().getDefaultValuesInsertPart());
	}

	private ColumnVisitor createColumnVisitor(RenderContext context) {

		return new ColumnVisitor(context, false, it -> {

			if (columns.length() != 0) {
				columns.append(", ");
			}

			columns.append(it);
		});
	}

	private IntoClauseVisitor createIntoClauseVisitor(RenderContext context) {

		return new IntoClauseVisitor(context, it -> {

			if (into.length() != 0) {
				into.append(", ");
			}

			into.append(it);
		});
	}
}
