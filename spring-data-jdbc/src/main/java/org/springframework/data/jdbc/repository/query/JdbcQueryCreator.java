/*
 * Copyright 2020-2025 the original author or authors.
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

import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.repository.Lock;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RelationalQueryCreator} that creates {@link ParametrizedQuery} from a {@link PartTree}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @author Diego Krupitza
 * @author Artemiy Degtyarev
 * @since 2.0
 */
public class JdbcQueryCreator extends RelationalQueryCreator<ParametrizedQuery> {

	private final RelationalMappingContext context;
	private final PartTree tree;
	private final RelationalParameterAccessor accessor;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final boolean isSliceQuery;
	private final ReturnedType returnedType;
	private final Optional<Lock> lockMode;
	private final StatementFactory statementFactory;
	private final boolean isScrollQuery;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link JdbcConverter}, {@link Dialect},
	 * {@link RelationalEntityMetadata} and {@link RelationalParameterAccessor}.
	 *
	 * @param context the mapping context. Must not be {@literal null}.
	 * @param tree part tree, must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 * @param isSliceQuery flag denoting if the query returns a {@link org.springframework.data.domain.Slice}.
	 * @param returnedType the {@link ReturnedType} to be returned by the query. Must not be {@literal null}.
	 * @deprecated use
	 *             {@link JdbcQueryCreator#JdbcQueryCreator(RelationalMappingContext, PartTree, JdbcConverter, Dialect, RelationalEntityMetadata, RelationalParameterAccessor, boolean, ReturnedType, Optional, SqlGeneratorSource, boolean)}
	 *             instead.
	 */
	@Deprecated(since = "4.0", forRemoval = true)
	JdbcQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor, boolean isSliceQuery,
			ReturnedType returnedType, Optional<Lock> lockMode, boolean isScrollQuery) {
		this(context, tree, converter, dialect, entityMetadata, accessor, isSliceQuery, returnedType, lockMode,
				new SqlGeneratorSource(context, converter, dialect), isScrollQuery);
	}

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link JdbcConverter}, {@link Dialect},
	 * {@link JdbcQueryMethod} and {@link RelationalParameterAccessor}.
	 *
	 * @param tree part tree, must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 * @param returnedType the {@link ReturnedType} to be returned by the query. Must not be {@literal null}.
	 * @since 4.0
	 */
	public JdbcQueryCreator(PartTree tree, JdbcConverter converter, Dialect dialect, JdbcQueryMethod queryMethod,
			RelationalParameterAccessor accessor, ReturnedType returnedType) {
		this(converter.getMappingContext(), tree, converter, dialect, queryMethod.getEntityInformation(), accessor,
				queryMethod.isSliceQuery(), returnedType, queryMethod.lookupLockAnnotation(),
				new SqlGeneratorSource(converter, dialect), queryMethod.isScrollQuery());
	}

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link JdbcConverter}, {@link Dialect},
	 * {@link RelationalEntityMetadata} and {@link RelationalParameterAccessor}.
	 *
	 * @param context the mapping context. Must not be {@literal null}.
	 * @param tree part tree, must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 * @param isSliceQuery flag denoting if the query returns a {@link org.springframework.data.domain.Slice}.
	 * @param returnedType the {@link ReturnedType} to be returned by the query. Must not be {@literal null}.
	 * @param lockMode lock mode to be used for the query.
	 * @param sqlGeneratorSource the source providing SqlGenerator instances for generating SQL. Must not be
	 *          {@literal null}
	 * @param isScrollQuery flag denoting if the query returns a {@link org.springframework.data.domain.Window}.
	 * @since 4.0
	 */
	public JdbcQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor, boolean isSliceQuery,
			ReturnedType returnedType, Optional<Lock> lockMode, SqlGeneratorSource sqlGeneratorSource,
			boolean isScrollQuery) {
		super(tree, accessor);

		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");
		Assert.notNull(returnedType, "ReturnedType must not be null");
		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");

		this.context = context;
		this.tree = tree;
		this.accessor = accessor;

		this.entityMetadata = entityMetadata;
		this.isSliceQuery = isSliceQuery;
		this.returnedType = returnedType;
		this.lockMode = lockMode;
		this.statementFactory = new StatementFactory(converter, dialect);
		this.isScrollQuery = isScrollQuery;
	}

	StatementFactory getStatementFactory() {
		return statementFactory;
	}

	/**
	 * Validate parameters for the derived query. Specifically checking that the query method defines scalar parameters
	 * and collection parameters where required and that invalid parameter declarations are rejected.
	 *
	 * @param tree the tree structure defining the predicate of the query.
	 * @param parameters parameters for the predicate.
	 */
	static void validate(PartTree tree, Parameters<?, ?> parameters, RelationalMappingContext context) {

		RelationalQueryCreator.validate(tree, parameters);

		for (PartTree.OrPart parts : tree) {
			for (Part part : parts) {

				PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath = context
						.getPersistentPropertyPath(part.getProperty());
				AggregatePath path = context.getAggregatePath(propertyPath);

				path.forEach(JdbcQueryCreator::validateProperty);
			}
		}
	}

	private static void validateProperty(AggregatePath path) {

		if (path.isRoot()) {
			return;
		}

		if (!path.getParentPath().isEmbedded() && path.getLength() > 2) {
			throw new IllegalArgumentException(String.format("Cannot query by nested property: %s", path.toDotPath()));
		}

		if (path.isMultiValued() || path.isMap()) {
			throw new IllegalArgumentException(
					String.format("Cannot query by multi-valued property: %s", path.getRequiredLeafProperty().getName()));
		}

		if (!path.isEmbedded() && path.isEntity()) {
			throw new IllegalArgumentException(String.format("Cannot query by nested entity: %s", path.toDotPath()));
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
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		StatementFactory.SelectionBuilder selection = getSelection(entity);

		selection.page(accessor.getPageable()).filter(criteria).orderBy(sort);

		selection.scrollPosition(accessor.getScrollPosition());

		if (this.lockMode.isPresent()) {
			selection.lock(this.lockMode.get().value());
		}

		String sql = selection.build(parameterSource);
		return new ParametrizedQuery(sql, parameterSource, criteria != null ? criteria : Criteria.empty());
	}

	StatementFactory.SelectionBuilder getSelection(RelationalPersistentEntity<?> entity) {

		if (tree.isExistsProjection()) {
			return statementFactory.exists(entity);
		} else if (tree.isCountProjection()) {
			return statementFactory.count(entity);
		}

		StatementFactory.SelectionBuilder selection;

		if (isSliceQuery) {
			selection = statementFactory.slice(entity);
		} else if (isScrollQuery) {
			selection = statementFactory.scroll(entity);
		} else {
			selection = statementFactory.select(entity);
		}

		if (returnedType.needsCustomConstruction()) {
			selection.project(returnedType.getInputProperties());
		}

		if (tree.isLimiting()) {
			selection.limit(tree.getResultLimit());
		}

		return selection;
	}

}
