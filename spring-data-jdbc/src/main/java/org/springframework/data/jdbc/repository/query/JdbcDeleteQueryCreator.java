/*
 * Copyright 2020-2024 the original author or authors.
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
import java.util.List;
import java.util.stream.Stream;

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMapper;
import org.springframework.data.mapping.Parameter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.DeleteBuilder.DeleteWhere;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectBuilder.SelectWhere;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RelationalQueryCreator} that creates {@link List} of deletion {@link ParametrizedQuery}
 * from a {@link PartTree}.
 *
 * @author Yunyoung LEE
 * @author Nikita Konev
 * @since 3.5
 */
class JdbcDeleteQueryCreator extends RelationalQueryCreator<List<ParametrizedQuery>> {

	private final RelationalMappingContext context;
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
	JdbcDeleteQueryCreator(RelationalMappingContext context, PartTree tree, JdbcConverter converter, Dialect dialect,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor) {

		super(tree, accessor);

		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");

		this.context = context;
		this.entityMetadata = entityMetadata;
		this.queryMapper = new QueryMapper(converter);
		this.renderContextFactory = new RenderContextFactory(dialect);
	}

	@Override
	protected List<ParametrizedQuery> complete(@Nullable Criteria criteria, Sort sort) {

		RelationalPersistentEntity<?> entity = entityMetadata.getTableEntity();
		Table table = Table.create(entityMetadata.getTableName());
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		SqlContext sqlContext = new SqlContext(entity);

		Condition condition = criteria == null ? null
				: queryMapper.getMappedObject(parameterSource, criteria, table, entity);

		// create select criteria query for subselect
		SelectWhere selectBuilder = StatementBuilder.select(sqlContext.getIdColumn()).from(table);
		Select select = condition == null ? selectBuilder.build() : selectBuilder.where(condition).build();

		// create delete relation queries
		List<Delete> deleteChain = new ArrayList<>();
		deleteRelations(deleteChain, entity, select);

		// crate delete query
		DeleteWhere deleteBuilder = StatementBuilder.delete(table);
		Delete delete = condition == null ? deleteBuilder.build() : deleteBuilder.where(condition).build();

		deleteChain.add(delete);

		SqlRenderer renderer = SqlRenderer.create(renderContextFactory.createRenderContext());

		List<ParametrizedQuery> queries = new ArrayList<>(deleteChain.size());
		for (Delete d : deleteChain) {
			queries.add(new ParametrizedQuery(renderer.render(d), parameterSource));
		}

		return queries;
	}

	private void deleteRelations(List<Delete> deleteChain, RelationalPersistentEntity<?> entity, Select parentSelect) {

		for (PersistentPropertyPath<RelationalPersistentProperty> path : context
				.findPersistentPropertyPaths(entity.getType(), p -> true)) {

			AggregatePath aggregatePath = context.getAggregatePath(path);

			// prevent duplication on recursive call
			if (path.getLength() > 1 && !aggregatePath.getParentPath().isEmbedded()) {
				continue;
			}

			if (aggregatePath.isEntity() && !aggregatePath.isEmbedded()) {

				SqlContext sqlContext = new SqlContext(aggregatePath.getLeafEntity());

				Condition inCondition = Conditions
						.in(sqlContext.getTable().column(aggregatePath.getTableInfo().reverseColumnInfo().name()), parentSelect);

				Select select = StatementBuilder.select( //
						sqlContext.getTable().column(aggregatePath.getIdDefiningParentPath().getTableInfo().idColumnName()) //
				).from(sqlContext.getTable()) //
						.where(inCondition) //
						.build();
				deleteRelations(deleteChain, aggregatePath.getLeafEntity(), select);

				deleteChain.add(StatementBuilder.delete(sqlContext.getTable()).where(inCondition).build());
			}
		}
	}
}
