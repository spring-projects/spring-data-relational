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
package org.springframework.data.jdbc.repository.query;

import lombok.Value;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RelationalQueryCreator} that creates {@link ParametrizedQuery} from a {@link PartTree}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 2.0
 */
class JdbcQueryCreator extends RelationalQueryCreator<ParametrizedQuery> {

	private final RelationalMappingContext context;
	private final PartTree tree;
	private final RelationalParameterAccessor accessor;
	private final QueryMapper queryMapper;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final RenderContextFactory renderContextFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link JdbcConverter}, {@link Dialect},
	 * {@link RelationalEntityMetadata} and {@link RelationalParameterAccessor}.
	 *
	 * @param context
	 * @param tree part tree, must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 */
	JdbcQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor) {
		super(tree, accessor);

		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");

		this.context = context;
		this.tree = tree;
		this.accessor = accessor;

		this.entityMetadata = entityMetadata;
		this.queryMapper = new QueryMapper(dialect, converter);
		this.renderContextFactory = new RenderContextFactory(dialect);
	}

	/**
	 * Validate parameters for the derived query. Specifically checking that the query method defines scalar parameters
	 * and collection parameters where required and that invalid parameter declarations are rejected.
	 *
	 * @param tree
	 * @param parameters
	 */
	static void validate(PartTree tree, Parameters<?, ?> parameters,
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context) {

		RelationalQueryCreator.validate(tree, parameters);

		for (PartTree.OrPart parts : tree) {
			for (Part part : parts) {

				PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath = context
						.getPersistentPropertyPath(part.getProperty());
				PersistentPropertyPathExtension path = new PersistentPropertyPathExtension(context, propertyPath);

				for (PersistentPropertyPathExtension pathToValidate = path; path.getLength() > 0; path = path.getParentPath()) {
					validateProperty(pathToValidate);
				}
			}
		}
	}

	private static void validateProperty(PersistentPropertyPathExtension path) {

		if (!path.getParentPath().isEmbedded() && path.getLength() > 1) {
			throw new IllegalArgumentException(
					String.format("Cannot query by nested property: %s", path.getRequiredPersistentPropertyPath().toDotPath()));
		}

		if (path.isMultiValued() || path.isMap()) {
			throw new IllegalArgumentException(String.format("Cannot query by multi-valued property: %s",
					path.getRequiredPersistentPropertyPath().getLeafProperty().getName()));
		}

		if (!path.isEmbedded() && path.isEntity()) {
			throw new IllegalArgumentException(
					String.format("Cannot query by nested entity: %s", path.getRequiredPersistentPropertyPath().toDotPath()));
		}

		if (path.getRequiredPersistentPropertyPath().getLeafProperty().isReference()) {
			throw new IllegalArgumentException(
					String.format("Cannot query by reference: %s", path.getRequiredPersistentPropertyPath().toDotPath()));
		}
	}

	/**
	 * Creates {@link ParametrizedQuery} applying the given {@link Criteria} and {@link Sort} definition.
	 *
	 * @param criteria {@link Criteria} to be applied to query
	 * @param sort sort option to be applied to query, must not be {@literal null}.
	 * @return instance of {@link ParametrizedQuery}
	 */
	@Override
	protected ParametrizedQuery complete(@Nullable Criteria criteria, Sort sort) {

		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();
		Table table = Table.create(entityMetadata.getTableName());
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		SelectBuilder.SelectLimitOffset limitOffsetBuilder = createSelectClause(entity, table);
		SelectBuilder.SelectWhere whereBuilder = applyLimitAndOffset(limitOffsetBuilder);
		SelectBuilder.SelectOrdered selectOrderBuilder = applyCriteria(criteria, entity, table, parameterSource,
				whereBuilder);
		selectOrderBuilder = applyOrderBy(sort, entity, table, selectOrderBuilder);

		Select select = selectOrderBuilder.build();

		String sql = SqlRenderer.create(renderContextFactory.createRenderContext()).render(select);

		return new ParametrizedQuery(sql, parameterSource);
	}

	private SelectBuilder.SelectOrdered applyOrderBy(Sort sort, RelationalPersistentEntity<?> entity, Table table,
			SelectBuilder.SelectOrdered selectOrdered) {

		return sort.isSorted() ? //
				selectOrdered.orderBy(queryMapper.getMappedSort(table, sort, entity)) //
				: selectOrdered;
	}

	private SelectBuilder.SelectOrdered applyCriteria(@Nullable Criteria criteria, RelationalPersistentEntity<?> entity,
			Table table, MapSqlParameterSource parameterSource, SelectBuilder.SelectWhere whereBuilder) {

		return criteria != null //
				? whereBuilder.where(queryMapper.getMappedObject(parameterSource, criteria, table, entity)) //
				: whereBuilder;
	}

	private SelectBuilder.SelectWhere applyLimitAndOffset(SelectBuilder.SelectLimitOffset limitOffsetBuilder) {

		if (tree.isExistsProjection()) {
			limitOffsetBuilder = limitOffsetBuilder.limit(1);
		} else if (tree.isLimiting()) {
			limitOffsetBuilder = limitOffsetBuilder.limit(tree.getMaxResults());
		}

		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			limitOffsetBuilder = limitOffsetBuilder.limit(pageable.getPageSize()).offset(pageable.getOffset());
		}

		return (SelectBuilder.SelectWhere) limitOffsetBuilder;
	}

	private SelectBuilder.SelectLimitOffset createSelectClause(RelationalPersistentEntity<?> entity, Table table) {

		SelectBuilder.SelectJoin builder;
		if (tree.isExistsProjection()) {

			Column idColumn = table.column(entity.getIdColumn());
			builder = Select.builder().select(idColumn).from(table);
		} else {
			builder = selectBuilder(table);
		}

		return (SelectBuilder.SelectLimitOffset) builder;
	}

	private SelectBuilder.SelectJoin selectBuilder(Table table) {

		List<Expression> columnExpressions = new ArrayList<>();
		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();
		SqlContext sqlContext = new SqlContext(entity);

		List<Join> joinTables = new ArrayList<>();
		for (PersistentPropertyPath<RelationalPersistentProperty> path : context
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			PersistentPropertyPathExtension extPath = new PersistentPropertyPathExtension(context, path);

			// add a join if necessary
			Join join = getJoin(sqlContext, extPath);
			if (join != null) {
				joinTables.add(join);
			}

			Column column = getColumn(sqlContext, extPath);
			if (column != null) {
				columnExpressions.add(column);
			}
		}

		SelectBuilder.SelectAndFrom selectBuilder = StatementBuilder.select(columnExpressions);
		SelectBuilder.SelectJoin baseSelect = selectBuilder.from(table);

		for (Join join : joinTables) {
			baseSelect = baseSelect.leftOuterJoin(join.joinTable).on(join.joinColumn).equals(join.parentId);
		}

		return baseSelect;
	}

	/**
	 * Create a {@link Column} for {@link PersistentPropertyPathExtension}.
	 *
	 * @param sqlContext
	 * @param path the path to the column in question.
	 * @return the statement as a {@link String}. Guaranteed to be not {@literal null}.
	 */
	@Nullable
	private Column getColumn(SqlContext sqlContext, PersistentPropertyPathExtension path) {

		// an embedded itself doesn't give an column, its members will though.
		// if there is a collection or map on the path it won't get selected at all, but it will get loaded with a separate
		// select
		// only the parent path is considered in order to handle arrays that get stored as BINARY properly
		if (path.isEmbedded() || path.getParentPath().isMultiValued()) {
			return null;
		}

		if (path.isEntity()) {

			// Simple entities without id include there backreference as an synthetic id in order to distinguish null entities
			// from entities with only null values.

			if (path.isQualified() //
					|| path.isCollectionLike() //
					|| path.hasIdProperty() //
			) {
				return null;
			}

			return sqlContext.getReverseColumn(path);
		}

		return sqlContext.getColumn(path);
	}

	@Nullable
	Join getJoin(SqlContext sqlContext, PersistentPropertyPathExtension path) {

		if (!path.isEntity() || path.isEmbedded() || path.isMultiValued()) {
			return null;
		}

		Table currentTable = sqlContext.getTable(path);

		PersistentPropertyPathExtension idDefiningParentPath = path.getIdDefiningParentPath();
		Table parentTable = sqlContext.getTable(idDefiningParentPath);

		return new Join( //
				currentTable, //
				currentTable.column(path.getReverseColumnName()), //
				parentTable.column(idDefiningParentPath.getIdColumnName()) //
		);
	}

	/**
	 * Value object representing a {@code JOIN} association.
	 */
	@Value
	static private class Join {
		Table joinTable;
		Column joinColumn;
		Column parentId;
	}
}
