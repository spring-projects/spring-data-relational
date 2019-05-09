/*
 * Copyright 2019 the original author or authors.
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

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.BindTarget;
import org.springframework.data.r2dbc.dialect.Bindings;
import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.query.BoundAssignments;
import org.springframework.data.r2dbc.query.BoundCondition;
import org.springframework.data.r2dbc.query.UpdateMapper;
import org.springframework.data.r2dbc.support.StatementRenderUtil;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.AssignValue;
import org.springframework.data.relational.core.sql.Assignment;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.DeleteBuilder;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.InsertBuilder;
import org.springframework.data.relational.core.sql.InsertBuilder.InsertValuesWithBuild;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Update;
import org.springframework.data.relational.core.sql.UpdateBuilder;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default {@link StatementMapper} implementation.
 *
 * @author Mark Paluch
 */
@RequiredArgsConstructor
class DefaultStatementMapper implements StatementMapper {

	private final Dialect dialect;
	private final RenderContext renderContext;
	private final UpdateMapper updateMapper;
	private final MappingContext<RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementMapper#forType(java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> TypedStatementMapper<T> forType(Class<T> type) {

		Assert.notNull(type, "Type must not be null!");

		return new DefaultTypedStatementMapper<>(
				(RelationalPersistentEntity<T>) this.mappingContext.getRequiredPersistentEntity(type));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.SelectSpec)
	 */
	@Override
	public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
		return getMappedObject(selectSpec, null);
	}

	private PreparedOperation<Select> getMappedObject(SelectSpec selectSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		Table table = Table.create(selectSpec.getTable());
		List<Column> columns = table.columns(selectSpec.getProjectedFields());
		SelectBuilder.SelectFromAndJoin selectBuilder = StatementBuilder.select(columns).from(table);

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Bindings bindings = Bindings.empty();

		if (selectSpec.getCriteria() != null) {

			BoundCondition mappedObject = this.updateMapper.getMappedObject(bindMarkers, selectSpec.getCriteria(), table,
					entity);

			bindings = mappedObject.getBindings();
			selectBuilder.where(mappedObject.getCondition());
		}

		if (selectSpec.getSort().isSorted()) {

			Sort mappedSort = this.updateMapper.getMappedObject(selectSpec.getSort(), entity);
			selectBuilder.orderBy(createOrderByFields(table, mappedSort));
		}

		OptionalLong limit;
		OptionalLong offset;

		if (selectSpec.getPage().isPaged()) {

			Pageable page = selectSpec.getPage();
			limit = OptionalLong.of(page.getPageSize());
			offset = OptionalLong.of(page.getOffset());
		} else {
			limit = OptionalLong.empty();
			offset = OptionalLong.empty();
		}

		Select select = selectBuilder.build();
		return new DefaultPreparedOperation<Select>(select, this.renderContext, bindings) {
			@Override
			public String toQuery() {
				return StatementRenderUtil.render(select, limit, offset, DefaultStatementMapper.this.dialect);
			}
		};
	}

	private Collection<? extends OrderByField> createOrderByFields(Table table, Sort sortToUse) {

		List<OrderByField> fields = new ArrayList<>();

		for (Sort.Order order : sortToUse) {

			OrderByField orderByField = OrderByField.from(table.column(order.getProperty()));

			if (order.getDirection() != null) {
				fields.add(order.isAscending() ? orderByField.asc() : orderByField.desc());
			} else {
				fields.add(orderByField);
			}
		}

		return fields;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.InsertSpec)
	 */
	@Override
	public PreparedOperation<Insert> getMappedObject(InsertSpec insertSpec) {
		return getMappedObject(insertSpec, null);
	}

	private PreparedOperation<Insert> getMappedObject(InsertSpec insertSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(insertSpec.getTable());

		BoundAssignments boundAssignments = this.updateMapper.getMappedObject(bindMarkers, insertSpec.getAssignments(),
				table, entity);

		Bindings bindings;

		if (boundAssignments.getAssignments().isEmpty()) {
			throw new IllegalStateException("INSERT contains no values");
		}

		bindings = boundAssignments.getBindings();

		InsertBuilder.InsertIntoColumnsAndValues insertBuilder = StatementBuilder.insert(table);
		InsertValuesWithBuild withBuild = (InsertValuesWithBuild) insertBuilder;

		for (Assignment assignment : boundAssignments.getAssignments()) {

			if (assignment instanceof AssignValue) {
				AssignValue assignValue = (AssignValue) assignment;

				insertBuilder.column(assignValue.getColumn());
				withBuild = insertBuilder.value(assignValue.getValue());
			}
		}

		return new DefaultPreparedOperation<>(withBuild.build(), this.renderContext, bindings);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.UpdateSpec)
	 */
	@Override
	public PreparedOperation<Update> getMappedObject(UpdateSpec updateSpec) {
		return getMappedObject(updateSpec, null);
	}

	private PreparedOperation<Update> getMappedObject(UpdateSpec updateSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(updateSpec.getTable());

		BoundAssignments boundAssignments = this.updateMapper.getMappedObject(bindMarkers,
				updateSpec.getUpdate().getAssignments(), table, entity);

		Bindings bindings;

		if (boundAssignments.getAssignments().isEmpty()) {
			throw new IllegalStateException("UPDATE contains no assignments");
		}

		bindings = boundAssignments.getBindings();

		UpdateBuilder.UpdateWhere updateBuilder = StatementBuilder.update(table).set(boundAssignments.getAssignments());

		Update update;

		if (updateSpec.getCriteria() != null) {

			BoundCondition boundCondition = this.updateMapper.getMappedObject(bindMarkers, updateSpec.getCriteria(), table,
					entity);

			bindings = bindings.and(boundCondition.getBindings());
			update = updateBuilder.where(boundCondition.getCondition()).build();
		} else {
			update = updateBuilder.build();
		}

		return new DefaultPreparedOperation<>(update, this.renderContext, bindings);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.DeleteSpec)
	 */
	@Override
	public PreparedOperation<Delete> getMappedObject(DeleteSpec deleteSpec) {
		return getMappedObject(deleteSpec, null);
	}

	private PreparedOperation<Delete> getMappedObject(DeleteSpec deleteSpec,
			@Nullable RelationalPersistentEntity<?> entity) {

		BindMarkers bindMarkers = this.dialect.getBindMarkersFactory().create();
		Table table = Table.create(deleteSpec.getTable());

		DeleteBuilder.DeleteWhere deleteBuilder = StatementBuilder.delete(table);

		Bindings bindings = Bindings.empty();

		Delete delete;
		if (deleteSpec.getCriteria() != null) {

			BoundCondition boundCondition = this.updateMapper.getMappedObject(bindMarkers, deleteSpec.getCriteria(), table,
					entity);

			bindings = boundCondition.getBindings();
			delete = deleteBuilder.where(boundCondition.getCondition()).build();
		} else {
			delete = deleteBuilder.build();
		}

		return new DefaultPreparedOperation<>(delete, this.renderContext, bindings);
	}

	/**
	 * Default implementation of {@link PreparedOperation}.
	 *
	 * @param <T>
	 */
	@RequiredArgsConstructor
	static class DefaultPreparedOperation<T> implements PreparedOperation<T> {

		private final T source;
		private final RenderContext renderContext;
		private final Bindings bindings;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.PreparedOperation#getSource()
		 */
		@Override
		public T getSource() {
			return this.source;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.QueryOperation#toQuery()
		 */
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

	@RequiredArgsConstructor
	class DefaultTypedStatementMapper<T> implements TypedStatementMapper<T> {

		final RelationalPersistentEntity<T> entity;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementMapper#forType(java.lang.Class)
		 */
		@Override
		public <TC> TypedStatementMapper<TC> forType(Class<TC> type) {
			return DefaultStatementMapper.this.forType(type);
		}

		/*
		* (non-Javadoc)
		* @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.SelectSpec)
		*/
		@Override
		public PreparedOperation<?> getMappedObject(SelectSpec selectSpec) {
			return DefaultStatementMapper.this.getMappedObject(selectSpec, this.entity);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.InsertSpec)
		 */
		@Override
		public PreparedOperation<?> getMappedObject(InsertSpec insertSpec) {
			return DefaultStatementMapper.this.getMappedObject(insertSpec, this.entity);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.UpdateSpec)
		 */
		@Override
		public PreparedOperation<?> getMappedObject(UpdateSpec updateSpec) {
			return DefaultStatementMapper.this.getMappedObject(updateSpec, this.entity);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.function.StatementMapper#getMappedObject(org.springframework.data.r2dbc.function.StatementMapper.DeleteSpec)
		 */
		@Override
		public PreparedOperation<?> getMappedObject(DeleteSpec deleteSpec) {
			return DefaultStatementMapper.this.getMappedObject(deleteSpec, this.entity);
		}
	}
}
