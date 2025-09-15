/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMapper;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.util.Predicates;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Contract;

/**
 * Utility to render SQL statements for entities, count/exists projections, and slice queries. This is an internal
 * utility and should not be used outside of the framework as it can change without deprecation notice.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class StatementFactory {

	private final JdbcConverter converter;
	private final RenderContextFactory renderContextFactory;
	private final QueryMapper queryMapper;
	private final SqlGeneratorSource sqlGeneratorSource;

	public StatementFactory(JdbcConverter converter, Dialect dialect) {
		this.renderContextFactory = new RenderContextFactory(dialect);
		this.converter = converter;
		this.queryMapper = new QueryMapper(converter);
		this.sqlGeneratorSource = new SqlGeneratorSource(converter, dialect);
	}

	public SelectionBuilder select(Class<?> entity) {
		return select(converter.getMappingContext().getRequiredPersistentEntity(entity));
	}

	public SelectionBuilder select(RelationalPersistentEntity<?> entity) {
		return new SelectionBuilder(entity, SelectionBuilder.Mode.SELECT);
	}

	public SelectionBuilder count(Class<?> entity) {
		return count(converter.getMappingContext().getRequiredPersistentEntity(entity));
	}

	public SelectionBuilder count(RelationalPersistentEntity<?> entity) {
		return new SelectionBuilder(entity, SelectionBuilder.Mode.COUNT);
	}

	public SelectionBuilder exists(Class<?> entity) {
		return exists(converter.getMappingContext().getRequiredPersistentEntity(entity));
	}

	public SelectionBuilder exists(RelationalPersistentEntity<?> entity) {
		return new SelectionBuilder(entity, SelectionBuilder.Mode.EXISTS);
	}

	public SelectionBuilder slice(Class<?> entity) {
		return slice(converter.getMappingContext().getRequiredPersistentEntity(entity));
	}

	public SelectionBuilder slice(RelationalPersistentEntity<?> entity) {
		return new SelectionBuilder(entity, SelectionBuilder.Mode.SLICE);
	}

	public class SelectionBuilder {

		private final RelationalPersistentEntity<?> entity;
		private final Table table;
		private final Mode mode;

		private @Nullable LockMode lockMode;
		private Limit limit = Limit.unlimited();
		private Pageable pageable = Pageable.unpaged();
		private Sort sort = Sort.unsorted();
		private Criteria criteria = Criteria.empty();
		private List<String> properties = new ArrayList<>();

		private SelectionBuilder(RelationalPersistentEntity<?> entity, Mode mode) {
			this.entity = entity;
			this.table = Table.create(entity.getTableName());
			this.mode = mode;
		}

		@Contract("_ -> this")
		public SelectionBuilder project(Collection<String> properties) {
			this.properties = List.copyOf(properties);
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder project(String... properties) {
			this.properties = Arrays.asList(properties);
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder orderBy(Sort sort) {
			this.sort = this.sort.and(sort);
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder page(@Nullable Pageable pageable) {

			if (pageable != null) {
				this.pageable = pageable;
				orderBy(pageable.getSort());
			}
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder limit(int limit) {
			this.limit = Limit.of(limit);
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder limit(Limit limit) {
			this.limit = limit;
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder filter(@Nullable Criteria criteria) {
			this.criteria = criteria == null ? Criteria.empty() : criteria;
			return this;
		}

		@Contract("_ -> this")
		public SelectionBuilder lock(LockMode lockMode) {
			this.lockMode = lockMode;
			return this;
		}

		public String build(MapSqlParameterSource parameterSource) {

			SelectBuilder.SelectLimitOffset limitOffsetBuilder = createSelectClause(entity, table);
			SelectBuilder.SelectWhere whereBuilder = applyLimitAndOffset(limitOffsetBuilder);
			SelectBuilder.SelectOrdered selectOrderBuilder = applyCriteria(criteria, entity, table, parameterSource,
					whereBuilder);
			selectOrderBuilder = applyOrderBy(sort, entity, table, selectOrderBuilder);

			SelectBuilder.BuildSelect completedBuildSelect = selectOrderBuilder;
			if (this.lockMode != null) {
				completedBuildSelect = selectOrderBuilder.lock(this.lockMode);
			}

			Select select = completedBuildSelect.build();

			return SqlRenderer.create(renderContextFactory.createRenderContext()).render(select);
		}

		SelectBuilder.SelectOrdered applyOrderBy(Sort sort, RelationalPersistentEntity<?> entity, Table table,
				SelectBuilder.SelectOrdered selectOrdered) {

			return sort.isSorted() ? //
					selectOrdered.orderBy(queryMapper.getMappedSort(table, sort, entity)) //
					: selectOrdered;
		}

		SelectBuilder.SelectOrdered applyCriteria(@Nullable Criteria criteria, RelationalPersistentEntity<?> entity,
				Table table, MapSqlParameterSource parameterSource, SelectBuilder.SelectWhere whereBuilder) {

			return criteria != null && !criteria.isEmpty() //
					? whereBuilder.where(queryMapper.getMappedObject(parameterSource, criteria, table, entity)) //
					: whereBuilder;
		}

		SelectBuilder.SelectWhere applyLimitAndOffset(SelectBuilder.SelectLimitOffset limitOffsetBuilder) {

			if (mode == Mode.COUNT) {
				return (SelectBuilder.SelectWhere) limitOffsetBuilder;
			}

			if (mode == Mode.EXISTS) {
				limitOffsetBuilder = limitOffsetBuilder.limit(1);
			} else if (limit.isLimited()) {
				limitOffsetBuilder = limitOffsetBuilder.limit(limit.max());
			}

			if (pageable.isPaged()) {
				limitOffsetBuilder = limitOffsetBuilder
						.limit(mode == Mode.SLICE ? pageable.getPageSize() + 1 : pageable.getPageSize())
						.offset(pageable.getOffset());
			}

			return (SelectBuilder.SelectWhere) limitOffsetBuilder;
		}

		SelectBuilder.SelectLimitOffset createSelectClause(RelationalPersistentEntity<?> entity, Table table) {

			SelectBuilder.SelectJoin builder;

			if (mode == Mode.EXISTS) {
				AggregatePath.ColumnInfo anyIdColumnInfo = converter.getMappingContext().getAggregatePath(entity).getTableInfo()
						.idColumnInfos().any();
				Column idColumn = table.column(anyIdColumnInfo.name());
				builder = Select.builder().select(idColumn).from(table);
			} else if (mode == Mode.COUNT) {
				builder = Select.builder().select(Functions.count(Expressions.asterisk())).from(table);
			} else {
				builder = selectBuilder(table);
			}

			return (SelectBuilder.SelectLimitOffset) builder;
		}

		private SelectBuilder.SelectJoin selectBuilder(Table table) {

			Predicate<AggregatePath> filter;

			if (properties.isEmpty()) {
				filter = Predicates.isFalse();
			} else {
				filter = ap -> !properties.contains(ap.getRequiredBaseProperty().getName());
			}

			return (SelectBuilder.SelectJoin) sqlGeneratorSource.getSqlGenerator(entity.getType()).createSelectBuilder(table,
					filter);
		}

		enum Mode {
			COUNT, EXISTS, SELECT, SLICE
		}
	}
}
