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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.util.Assert;

/**
 * Mapper for statement specifications to {@link PreparedOperation}. Statement mapping applies a
 * {@link org.springframework.data.r2dbc.dialect.R2dbcDialect}-specific transformation considering
 * {@link org.springframework.r2dbc.core.binding.BindMarkers} and vendor-specific SQL differences.
 * <p>
 * {@link PreparedOperation Mapped statements} can be used directly with
 * {@link org.springframework.r2dbc.core.DatabaseClient#sql(Supplier)} without specifying further SQL or bindings as the
 * prepared operation encapsulates the specified SQL operation.
 *
 * @author Mark Paluch
 * @author Roman Chigvintsev
 * @author Mingyuan Wu
 * @author Diego Krupitza
 */
public interface StatementMapper {

	/**
	 * Create a new {@link StatementMapper} given {@link R2dbcDialect} and {@link R2dbcConverter}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @return the new {@link StatementMapper}.
	 * @since 1.2
	 */
	static StatementMapper create(R2dbcDialect dialect, R2dbcConverter converter) {

		Assert.notNull(dialect, "R2dbcDialect must not be null");
		Assert.notNull(converter, "R2dbcConverter must not be null");

		return new DefaultStatementMapper(dialect, converter);
	}

	/**
	 * Create a typed {@link StatementMapper} that considers type-specific mapping metadata.
	 *
	 * @param type must not be {@literal null}.
	 * @param <T>
	 * @return the typed {@link StatementMapper}.
	 */
	<T> TypedStatementMapper<T> forType(Class<T> type);

	/**
	 * Map a select specification to a {@link PreparedOperation}.
	 *
	 * @param selectSpec the insert operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link SelectSpec}.
	 */
	PreparedOperation<?> getMappedObject(SelectSpec selectSpec);

	/**
	 * Map a insert specification to a {@link PreparedOperation}.
	 *
	 * @param insertSpec the insert operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link InsertSpec}.
	 */
	PreparedOperation<?> getMappedObject(InsertSpec insertSpec);

	/**
	 * Map a update specification to a {@link PreparedOperation}.
	 *
	 * @param updateSpec the update operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link UpdateSpec}.
	 */
	PreparedOperation<?> getMappedObject(UpdateSpec updateSpec);

	/**
	 * Map a delete specification to a {@link PreparedOperation}.
	 *
	 * @param deleteSpec the update operation definition, must not be {@literal null}.
	 * @return the {@link PreparedOperation} for {@link DeleteSpec}.
	 */
	PreparedOperation<?> getMappedObject(DeleteSpec deleteSpec);

	/**
	 * Extension to {@link StatementMapper} that is associated with a type.
	 *
	 * @param <T>
	 */
	interface TypedStatementMapper<T> extends StatementMapper {}

	/**
	 * Create a {@code SELECT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link SelectSpec}.
	 */
	default SelectSpec createSelect(String table) {
		return SelectSpec.create(table);
	}

	/**
	 * Create a {@code SELECT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link SelectSpec}.
	 * @since 1.1
	 */
	default SelectSpec createSelect(SqlIdentifier table) {
		return SelectSpec.create(table);
	}

	/**
	 * Create an {@code INSERT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link InsertSpec}.
	 */
	default InsertSpec createInsert(String table) {
		return InsertSpec.create(table);
	}

	/**
	 * Create an {@code INSERT} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link InsertSpec}.
	 * @since 1.1
	 */
	default InsertSpec createInsert(SqlIdentifier table) {
		return InsertSpec.create(table);
	}

	/**
	 * Create an {@code UPDATE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link UpdateSpec}.
	 */
	default UpdateSpec createUpdate(String table, org.springframework.data.relational.core.query.Update update) {
		return UpdateSpec.create(table, update);
	}

	/**
	 * Create an {@code UPDATE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link UpdateSpec}.
	 * @since 1.1
	 */
	default UpdateSpec createUpdate(SqlIdentifier table, org.springframework.data.relational.core.query.Update update) {
		return UpdateSpec.create(table, update);
	}

	/**
	 * Create a {@code DELETE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link DeleteSpec}.
	 */
	default DeleteSpec createDelete(String table) {
		return DeleteSpec.create(table);
	}

	/**
	 * Create a {@code DELETE} specification for {@code table}.
	 *
	 * @param table
	 * @return the {@link DeleteSpec}.
	 * @since 1.1
	 */
	default DeleteSpec createDelete(SqlIdentifier table) {
		return DeleteSpec.create(table);
	}

	/**
	 * Returns {@link RenderContext}.
	 *
	 * @return {@link RenderContext} instance or {@literal null} if {@link RenderContext} is not available
	 */
	@Nullable
	default RenderContext getRenderContext() {
		return null;
	}

	/**
	 * {@code SELECT} specification.
	 */
	class SelectSpec {

		private final Table table;
		private final List<String> projectedFields;
		private final List<Expression> selectList;
		private final @Nullable CriteriaDefinition criteria;
		private final Sort sort;
		private final long offset;
		private final int limit;
		private final boolean distinct;
		private final LockMode lockMode;

		protected SelectSpec(Table table, List<String> projectedFields, List<Expression> selectList,
				@Nullable CriteriaDefinition criteria, Sort sort, int limit, long offset, boolean distinct, LockMode lockMode) {
			this.table = table;
			this.projectedFields = projectedFields;
			this.selectList = selectList;
			this.criteria = criteria;
			this.sort = sort;
			this.offset = offset;
			this.limit = limit;
			this.distinct = distinct;
			this.lockMode = lockMode;
		}

		/**
		 * Create an {@code SELECT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link SelectSpec}.
		 */
		public static SelectSpec create(String table) {
			return create(SqlIdentifier.unquoted(table));
		}

		/**
		 * Create an {@code SELECT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public static SelectSpec create(SqlIdentifier table) {

			List<String> projectedFields = Collections.emptyList();
			List<Expression> selectList = Collections.emptyList();
			return new SelectSpec(Table.create(table), projectedFields, selectList, Criteria.empty(), Sort.unsorted(), -1, -1,
					false, null);
		}

		public SelectSpec doWithTable(BiFunction<Table, SelectSpec, SelectSpec> function) {
			return function.apply(getTable(), this);
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(String... projectedFields) {
			return withProjection(Arrays.stream(projectedFields).map(table::column).collect(Collectors.toList()));
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(SqlIdentifier... projectedFields) {
			return withProjection(Arrays.stream(projectedFields).map(table::column).collect(Collectors.toList()));
		}

		/**
		 * Associate {@code expressions} with the select list and create a new {@link SelectSpec}.
		 *
		 * @param expressions
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(Expression... expressions) {

			List<Expression> selectList = new ArrayList<>(this.selectList);
			selectList.addAll(Arrays.asList(expressions));

			return new SelectSpec(this.table, projectedFields, selectList, this.criteria, this.sort, this.limit, this.offset,
					this.distinct, this.lockMode);
		}

		/**
		 * Associate {@code projectedFields} with the select and create a new {@link SelectSpec}.
		 *
		 * @param projectedFields
		 * @return the {@link SelectSpec}.
		 * @since 1.1
		 */
		public SelectSpec withProjection(Collection<Expression> projectedFields) {

			List<Expression> selectList = new ArrayList<>(this.selectList);
			selectList.addAll(projectedFields);

			return new SelectSpec(this.table, this.projectedFields, selectList, this.criteria, this.sort, this.limit,
					this.offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate a {@link Criteria} with the select and return a new {@link SelectSpec}.
		 *
		 * @param criteria
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withCriteria(CriteriaDefinition criteria) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, criteria, this.sort, this.limit,
					this.offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate {@link Sort} with the select and create a new {@link SelectSpec}.
		 *
		 * @param sort
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withSort(Sort sort) {

			if (sort.isSorted()) {
				return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, sort, this.limit,
						this.offset, this.distinct, this.lockMode);
			}

			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, this.limit,
					this.offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate a {@link Pageable} with the select and create a new {@link SelectSpec}.
		 *
		 * @param page
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec withPage(Pageable page) {

			if (page.isPaged()) {

				Sort sort = page.getSort();

				return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria,
						sort.isSorted() ? sort : this.sort, page.getPageSize(), page.getOffset(), this.distinct, this.lockMode);
			}

			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, this.limit,
					this.offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate a result offset with the select and create a new {@link SelectSpec}.
		 *
		 * @param offset
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec offset(long offset) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, this.limit,
					offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate a result limit with the select and create a new {@link SelectSpec}.
		 *
		 * @param limit
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec limit(int limit) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, limit,
					this.offset, this.distinct, this.lockMode);
		}

		/**
		 * Associate a result statement distinct with the select and create a new {@link SelectSpec}.
		 *
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec distinct() {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, limit,
					this.offset, true, this.lockMode);
		}

		/**
		 * Associate a lock mode with the select and create a new {@link SelectSpec}.
		 *
		 * @param lockMode the {@link LockMode} we want to use. This might be null
		 * @return the {@link SelectSpec}.
		 */
		public SelectSpec lock(LockMode lockMode) {
			return new SelectSpec(this.table, this.projectedFields, this.selectList, this.criteria, this.sort, limit,
					this.offset, this.distinct, lockMode);
		}

		/**
		 * The used lockmode
		 * 
		 * @return might be null if no lockmode defined.
		 */
		@Nullable
		public LockMode getLock() {
			return this.lockMode;
		}

		public Table getTable() {
			return this.table;
		}

		public List<Expression> getSelectList() {
			return Collections.unmodifiableList(selectList);
		}

		@Nullable
		public CriteriaDefinition getCriteria() {
			return this.criteria;
		}

		public Sort getSort() {
			return this.sort;
		}

		public long getOffset() {
			return this.offset;
		}

		public int getLimit() {
			return this.limit;
		}

		public boolean isDistinct() {
			return this.distinct;
		}

	}

	/**
	 * {@code INSERT} specification.
	 */
	class InsertSpec {

		private final SqlIdentifier table;
		private final Map<SqlIdentifier, Parameter> assignments;

		protected InsertSpec(SqlIdentifier table, Map<SqlIdentifier, Parameter> assignments) {
			this.table = table;
			this.assignments = assignments;
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 */
		public static InsertSpec create(String table) {
			return create(SqlIdentifier.unquoted(table));
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 * @since 1.1
		 */
		public static InsertSpec create(SqlIdentifier table) {
			return new InsertSpec(table, Collections.emptyMap());
		}

		/**
		 * Associate a column with a {@link Parameter} and create a new {@link InsertSpec}.
		 *
		 * @param column
		 * @param value
		 * @return the {@link InsertSpec}.
		 * @since 1.2
		 */
		public InsertSpec withColumn(String column, Parameter value) {
			return withColumn(SqlIdentifier.unquoted(column), value);
		}

		/**
		 * Associate a column with a {@link Parameter} and create a new {@link InsertSpec}.
		 *
		 * @param column
		 * @param value
		 * @return the {@link InsertSpec}.
		 * @since 1.2
		 */
		public InsertSpec withColumn(SqlIdentifier column, Parameter value) {

			Map<SqlIdentifier, Parameter> values = new LinkedHashMap<>(this.assignments);
			values.put(column, value);

			return new InsertSpec(this.table, values);
		}

		public SqlIdentifier getTable() {
			return this.table;
		}

		public Map<SqlIdentifier, Parameter> getAssignments() {
			return Collections.unmodifiableMap(this.assignments);
		}
	}

	/**
	 * {@code UPDATE} specification.
	 */
	class UpdateSpec {

		private final SqlIdentifier table;
		private final @Nullable org.springframework.data.relational.core.query.Update update;
		private final @Nullable CriteriaDefinition criteria;

		protected UpdateSpec(SqlIdentifier table, @Nullable org.springframework.data.relational.core.query.Update update,
				@Nullable CriteriaDefinition criteria) {

			this.table = table;
			this.update = update;
			this.criteria = criteria;
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 */
		public static UpdateSpec create(String table, org.springframework.data.relational.core.query.Update update) {
			return create(SqlIdentifier.unquoted(table), update);
		}

		/**
		 * Create an {@code INSERT} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link InsertSpec}.
		 * @since 1.1
		 */
		public static UpdateSpec create(SqlIdentifier table, org.springframework.data.relational.core.query.Update update) {
			return new UpdateSpec(table, update, Criteria.empty());
		}

		/**
		 * Associate a {@link Criteria} with the update and return a new {@link UpdateSpec}.
		 *
		 * @param criteria
		 * @return the {@link UpdateSpec}.
		 */
		public UpdateSpec withCriteria(CriteriaDefinition criteria) {
			return new UpdateSpec(this.table, this.update, criteria);
		}

		public SqlIdentifier getTable() {
			return this.table;
		}

		@Nullable
		public org.springframework.data.relational.core.query.Update getUpdate() {
			return this.update;
		}

		@Nullable
		public CriteriaDefinition getCriteria() {
			return this.criteria;
		}
	}

	/**
	 * {@code DELETE} specification.
	 */
	class DeleteSpec {

		private final SqlIdentifier table;
		private final @Nullable CriteriaDefinition criteria;

		protected DeleteSpec(SqlIdentifier table, CriteriaDefinition criteria) {
			this.table = table;
			this.criteria = criteria;
		}

		/**
		 * Create an {@code DELETE} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link DeleteSpec}.
		 */
		public static DeleteSpec create(String table) {
			return create(SqlIdentifier.unquoted(table));
		}

		/**
		 * Create an {@code DELETE} specification for {@code table}.
		 *
		 * @param table
		 * @return the {@link DeleteSpec}.
		 * @since 1.1
		 */
		public static DeleteSpec create(SqlIdentifier table) {
			return new DeleteSpec(table, Criteria.empty());
		}

		/**
		 * Associate a {@link Criteria} with the delete and return a new {@link DeleteSpec}.
		 *
		 * @param criteria
		 * @return the {@link DeleteSpec}.
		 */
		public DeleteSpec withCriteria(CriteriaDefinition criteria) {
			return new DeleteSpec(this.table, criteria);
		}

		public SqlIdentifier getTable() {
			return this.table;
		}

		@Nullable
		public CriteriaDefinition getCriteria() {
			return this.criteria;
		}
	}
}
