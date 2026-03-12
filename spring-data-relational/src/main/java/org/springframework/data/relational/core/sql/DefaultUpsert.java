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
package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.relational.core.dialect.InsertRenderContext;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.data.relational.core.sql.render.StandardSqlUpsertRenderContext;
import org.springframework.data.relational.core.sql.render.UpsertRenderContext;
import org.springframework.data.relational.core.sql.render.UpsertStatementVisitor;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 4.x
 */
class DefaultUpsert implements Upsert {

	private final Table table;
	private final List<Assignment> assignments;
	private final List<Column> conflictColumns;

	DefaultUpsert(Table table, List<Assignment> assignments, List<Column> conflictColumns) {

		this.table = table;
		this.assignments = new ArrayList<>(assignments);
		this.conflictColumns = new ArrayList<>(conflictColumns);
	}

	@Override
	public Table getTable() {
		return table;
	}

	@Override
	public List<Assignment> getAssignments() {
		return assignments;
	}

	@Override
	public List<Column> getConflictColumns() {
		return conflictColumns;
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null");

		visitor.enter(this);

		this.table.visit(visitor);
		this.conflictColumns.forEach(col -> col.visit(visitor));
		this.assignments.forEach(it -> it.visit(visitor));

		visitor.leave(this);
	}

	@Override
	public String toString() { // TODO: this method vs. SqlRenderer.toString(upsert);

		UpsertStatementVisitor visitor = new UpsertStatementVisitor(TO_STRING_ANSI_RENDER_CONTEXT);
		this.visit(visitor);
		return visitor.getRenderedPart().toString();
	}

	private static final RenderContext TO_STRING_ANSI_RENDER_CONTEXT = new RenderContext() {

		@Override
		public RenderNamingStrategy getNamingStrategy() {
			return NamingStrategies.asIs();
		}

		@Override
		public IdentifierProcessing getIdentifierProcessing() {
			return IdentifierProcessing.NONE;
		}

		@Override
		public SelectRenderContext getSelectRenderContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public InsertRenderContext getInsertRenderContext() {
			throw new UnsupportedOperationException();
		}

		@Override
		public UpsertRenderContext getUpsertRenderContext() {
			return StandardSqlUpsertRenderContext.INSTANCE;
		}
	};
}
