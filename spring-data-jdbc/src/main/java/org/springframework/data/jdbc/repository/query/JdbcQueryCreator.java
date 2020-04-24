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

import java.util.Collections;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RelationalQueryCreator} that creates {@link ParametrizedQuery} from a {@link PartTree}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class JdbcQueryCreator extends RelationalQueryCreator<ParametrizedQuery> {

	private final PartTree tree;
	private final RelationalParameterAccessor accessor;
	private final QueryMapper queryMapper;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final RenderContextFactory renderContextFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link JdbcConverter}, {@link Dialect},
	 * {@link RelationalEntityMetadata} and {@link RelationalParameterAccessor}.
	 *
	 * @param tree part tree, must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 */
	JdbcQueryCreator(PartTree tree, JdbcConverter converter, Dialect dialect, RelationalEntityMetadata<?> entityMetadata,
			RelationalParameterAccessor accessor) {
		super(tree, accessor);

		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");

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
	public static void validate(PartTree tree, Parameters<?, ?> parameters,
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
	protected ParametrizedQuery complete(Criteria criteria, Sort sort) {

		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();
		Table table = Table.create(entityMetadata.getTableName());
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		List<? extends Expression> columns = table.columns(getSelectProjection());
		if (columns.isEmpty()) {
			columns = Collections.singletonList(table.asterisk());
		}

		SelectBuilder.SelectFromAndJoin builder = Select.builder().select(columns).from(table);

		if (tree.isExistsProjection()) {
			builder = builder.limit(1);
		} else if (tree.isLimiting()) {
			builder = builder.limit(tree.getMaxResults());
		}

		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			builder = builder.limit(pageable.getPageSize()).offset(pageable.getOffset());
		}

		if (criteria != null) {
			builder.where(queryMapper.getMappedObject(parameterSource, criteria, table, entity));
		}

		if (sort.isSorted()) {
			builder.orderBy(queryMapper.getMappedSort(table, sort, entity));
		}

		Select select = builder.build();

		String sql = SqlRenderer.create(renderContextFactory.createRenderContext()).render(select);

		return new ParametrizedQuery(sql, parameterSource);
	}

	private SqlIdentifier[] getSelectProjection() {

		RelationalPersistentEntity<?> tableEntity = entityMetadata.getTableEntity();

		if (tree.isExistsProjection()) {
			return new SqlIdentifier[] { tableEntity.getIdColumn() };
		}

		return new SqlIdentifier[0];
	}
}
