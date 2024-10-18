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
package org.springframework.data.r2dbc.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.query.BoundAssignments;
import org.springframework.data.r2dbc.query.BoundCondition;
import org.springframework.data.r2dbc.query.UpdateMapper;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.core.sql.InsertBuilder.InsertValuesWithBuild;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.util.Assert;

/**
 * Default {@link StatementMapper} implementation.
 *
 * @author Mark Paluch
 * @author Roman Chigvintsev
 * @author Mingyuan Wu
 * @author Diego Krupitza
 */
class DefaultStatementMapper implements StatementMapper {

	private final R2dbcDialect dialect;
	private final RenderContext renderContext;
	private final UpdateMapper updateMapper;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	DefaultStatementMapper(R2dbcDialect dialect, R2dbcConverter converter) {

		RenderContextFactory factory = new RenderContextFactory(dialect);

		this.dialect = dialect;
		this.renderContext = factory.createRenderContext();
		this.updateMapper = new UpdateMapper(dialect, converter);
		this.mappingContext = converter.getMappingContext();
	}

	DefaultStatementMapper(R2dbcDialect dialect, RenderContext renderContext, UpdateMapper updateMapper,
			MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext) {
		this.dialect = dialect;
		this.renderContext = renderContext;
		this.updateMapper = updateMapper;
		this.mappingContext = mappingContext;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> TypedStatementMapper<T> forType(Class<T> type) {

		Assert.notNull(type, "Type must not be null");

		return new DefaultTypedStatementMapper<>(
				(RelationalPersistentEntity<T>) this.mappingContext.getRequiredPersistentEntity(type));
	}

	@Override
	public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
		return getMappedObject(selectSpec, null);
	}

	private PreparedOperation<Select> getMappedObject(SelectSpec selectSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		Table table = selectSpec.getTable();
		SelectBuilder.SelectAndFrom selectAndFrom = StatementBuilder.select(getSelectList(selectSpec, entity));

		if (selectSpec.isDistinct()) {
			selectAndFrom = selectAndFrom.distinct();
		}

		SelectBuilder.SelectFromAndJoin selectBuilder = selectAndFrom.from(table);

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Bindings bindings = Bindings.empty();
		CriteriaDefinition criteria = selectSpec.getCriteria();

		if (criteria != null && !criteria.isEmpty()) {

			BoundCondition mappedObject = this.updateMapper.getMappedObject(bindMarkers, criteria, table, entity);

			bindings = mappedObject.getBindings();
			selectBuilder.where(mappedObject.getCondition());
		}

		if (selectSpec.getSort().isSorted()) {

			List<OrderByField> sort = this.updateMapper.getMappedSort(table, selectSpec.getSort(), entity);
			selectBuilder.orderBy(sort);
		}

		if (selectSpec.getLimit() > 0) {
			selectBuilder.limit(selectSpec.getLimit());
		}

		if (selectSpec.getOffset() > 0) {
			selectBuilder.offset(selectSpec.getOffset());
		}

		if (selectSpec.getLock() != null) {
			selectBuilder.lock(selectSpec.getLock());
		}

		Select select = selectBuilder.build();
		return new DefaultPreparedOperation<>(select, this.renderContext, bindings);
	}

	protected List<Expression> getSelectList(SelectSpec selectSpec, @Nullable RelationalPersistentEntity<?> entity) {

		if (entity == null) {
			return selectSpec.getSelectList();
		}

		List<Expression> selectList = selectSpec.getSelectList();
		List<Expression> mapped = new ArrayList<>(selectList.size());

		for (Expression expression : selectList) {
			mapped.add(updateMapper.getMappedObject(expression, entity));
		}

		return mapped;
	}

	@Override
	public PreparedOperation<Insert> getMappedObject(InsertSpec insertSpec) {
		return getMappedObject(insertSpec, null);
	}

	private PreparedOperation<Insert> getMappedObject(InsertSpec insertSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(toSql(insertSpec.getTable()));

		BoundAssignments boundAssignments = this.updateMapper.getMappedObject(bindMarkers, insertSpec.getAssignments(),
				table, entity);

		Bindings bindings;

		bindings = boundAssignments.getBindings();

		InsertBuilder.InsertIntoColumnsAndValues insertBuilder = StatementBuilder.insert(table);
		InsertValuesWithBuild withBuild = (InsertValuesWithBuild) insertBuilder;

		for (Assignment assignment : boundAssignments.getAssignments()) {

			if (assignment instanceof AssignValue assignValue) {

				insertBuilder.column(assignValue.getColumn());
				withBuild = insertBuilder.value(assignValue.getValue());
			}
		}

		return new DefaultPreparedOperation<>(withBuild.build(), this.renderContext, bindings);
	}

	@Override
	public PreparedOperation<Update> getMappedObject(UpdateSpec updateSpec) {
		return getMappedObject(updateSpec, null);
	}

	private PreparedOperation<Update> getMappedObject(UpdateSpec updateSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(toSql(updateSpec.getTable()));

		if (updateSpec.getUpdate() == null || updateSpec.getUpdate().getAssignments().isEmpty()) {
			throw new IllegalArgumentException("UPDATE contains no assignments");
		}

		BoundAssignments boundAssignments = this.updateMapper.getMappedObject(bindMarkers,
				updateSpec.getUpdate().getAssignments(), table, entity);

		Bindings bindings;

		bindings = boundAssignments.getBindings();

		UpdateBuilder.UpdateWhere updateBuilder = StatementBuilder.update(table).set(boundAssignments.getAssignments());

		Update update;

		CriteriaDefinition criteria = updateSpec.getCriteria();
		if (criteria != null && !criteria.isEmpty()) {

			BoundCondition boundCondition = this.updateMapper.getMappedObject(bindMarkers, criteria, table, entity);

			bindings = bindings.and(boundCondition.getBindings());
			update = updateBuilder.where(boundCondition.getCondition()).build();
		} else {
			update = updateBuilder.build();
		}

		return new DefaultPreparedOperation<>(update, this.renderContext, bindings);
	}

	@Override
	public PreparedOperation<Delete> getMappedObject(DeleteSpec deleteSpec) {
		return getMappedObject(deleteSpec, null);
	}

	@Override
	public RenderContext getRenderContext() {
		return renderContext;
	}

	private PreparedOperation<Delete> getMappedObject(DeleteSpec deleteSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(toSql(deleteSpec.getTable()));

		DeleteBuilder.DeleteWhere deleteBuilder = StatementBuilder.delete(table);

		Bindings bindings = Bindings.empty();

		Delete delete;
		CriteriaDefinition criteria = deleteSpec.getCriteria();

		if (criteria != null && !criteria.isEmpty()) {

			BoundCondition boundCondition = this.updateMapper.getMappedObject(bindMarkers, deleteSpec.getCriteria(), table,
					entity);

			bindings = boundCondition.getBindings();
			delete = deleteBuilder.where(boundCondition.getCondition()).build();
		} else {
			delete = deleteBuilder.build();
		}

		return new DefaultPreparedOperation<>(delete, this.renderContext, bindings);
	}

	private String toSql(SqlIdentifier identifier) {

		Assert.notNull(identifier, "SqlIdentifier must not be null");

		return identifier.toSql(this.dialect.getIdentifierProcessing());
	}

	/**
	 * Default implementation of {@link PreparedOperation}.
	 *
	 * @param <T>
	 */
	static class DefaultPreparedOperation<T> implements PreparedOperation<T> {

		private final T source;
		private final RenderContext renderContext;
		private final Bindings bindings;

		DefaultPreparedOperation(T source, RenderContext renderContext, Bindings bindings) {

			this.source = source;
			this.renderContext = renderContext;
			this.bindings = bindings;
		}

		@Override
		public T getSource() {
			return this.source;
		}

		@Override
		public String toQuery() {

			SqlRenderer sqlRenderer = SqlRenderer.create(this.renderContext);

			if (this.source instanceof Select) {
				return sqlRenderer.render((Select) this.source);
			}

			if (this.source instanceof Insert) {
				return sqlRenderer.render((Insert) this.source);
			}

			if (this.source instanceof Update) {
				return sqlRenderer.render((Update) this.source);
			}

			if (this.source instanceof Delete) {
				return sqlRenderer.render((Delete) this.source);
			}

			throw new IllegalStateException("Cannot render " + this.getSource());
		}

		@Override
		public void bindTo(BindTarget to) {
			this.bindings.apply(to);
		}

	}

	class DefaultTypedStatementMapper<T> implements TypedStatementMapper<T> {

		final RelationalPersistentEntity<T> entity;

		DefaultTypedStatementMapper(RelationalPersistentEntity<T> entity) {
			this.entity = entity;
		}

		@Override
		public <TC> TypedStatementMapper<TC> forType(Class<TC> type) {
			return DefaultStatementMapper.this.forType(type);
		}

		@Override
		public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
			return DefaultStatementMapper.this.getMappedObject(selectSpec, this.entity);
		}

		@Override
		public PreparedOperation<?> getMappedObject(InsertSpec insertSpec) {
			return DefaultStatementMapper.this.getMappedObject(insertSpec, this.entity);
		}

		@Override
		public PreparedOperation<?> getMappedObject(UpdateSpec updateSpec) {
			return DefaultStatementMapper.this.getMappedObject(updateSpec, this.entity);
		}

		@Override
		public PreparedOperation<?> getMappedObject(DeleteSpec deleteSpec) {
			return DefaultStatementMapper.this.getMappedObject(deleteSpec, this.entity);
		}

		@Override
		public RenderContext getRenderContext() {
			return DefaultStatementMapper.this.getRenderContext();
		}
	}
}
