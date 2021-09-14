/*
 * Copyright 2019-2021 the original author or authors.
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

import org.jetbrains.annotations.NotNull;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Into;
import org.springframework.data.relational.core.sql.Values;
import org.springframework.data.relational.core.sql.Visitable;

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

	InsertStatementVisitor(RenderContext context) {
		this.intoClauseVisitor = createIntoClauseVisitor(context);
		this.columnVisitor = createColumnVisitor(context);
		this.valuesVisitor = new ValuesVisitor(context, values::append);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.DelegatingVisitor#doEnter(org.springframework.data.relational.core.sql.Visitable)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.DelegatingVisitor#doLeave(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	public Delegation doLeave(Visitable segment) {

		if (segment instanceof Insert) {

			builder.append("INSERT");

			builder.append(" INTO ").append(into);

			addInsertColumnsIfPresent();

			addInsertValuesIfPresentElseDefault((Insert) segment);

			return Delegation.leave();
		}

		return Delegation.retain();
	}

	private void addInsertValuesIfPresentElseDefault(Insert segment) {
		if (values.length() != 0) {
			builder.append(" VALUES (").append(values).append(")");
		} else {
			addInsertWithDefaultValuesToBuilder(segment.getInsertDialect());
		}
	}

	private void addInsertColumnsIfPresent() {
		if (columns.length() != 0) {
			builder.append(" (").append(columns).append(")");
		}
	}

	private void addInsertWithDefaultValuesToBuilder(Dialect dialect) {
		builder.append(dialect.getSqlInsertWithDefaultValues().getDefaultInsertPart());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.PartRenderer#getRenderedPart()
	 */
	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}

	@NotNull
	private ColumnVisitor createColumnVisitor(RenderContext context) {
		return new ColumnVisitor(context, false, it -> {

			if (columns.length() != 0) {
				columns.append(", ");
			}

			columns.append(it);
		});
	}

	@NotNull
	private IntoClauseVisitor createIntoClauseVisitor(RenderContext context) {
		return new IntoClauseVisitor(context, it -> {

			if (into.length() != 0) {
				into.append(", ");
			}

			into.append(it);
		});
	}
}
