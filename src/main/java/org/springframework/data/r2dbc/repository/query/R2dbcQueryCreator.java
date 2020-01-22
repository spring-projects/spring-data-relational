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
package org.springframework.data.r2dbc.repository.query;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.PreparedOperation;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Implementation of {@link AbstractQueryCreator} that creates {@link PreparedOperation} from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 */
public class R2dbcQueryCreator extends AbstractQueryCreator<PreparedOperation<?>, Criteria> {
	private final PartTree tree;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final RelationalEntityMetadata<?> entityMetadata;
	private final CriteriaFactory criteriaFactory;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link ReactiveDataAccessStrategy},
	 * {@link RelationalEntityMetadata} and {@link ParameterMetadataProvider}.
	 *
	 * @param tree part tree (must not be {@literal null})
	 * @param dataAccessStrategy data access strategy (must not be {@literal null})
	 * @param entityMetadata relational entity metadata (must not be {@literal null})
	 * @param parameterMetadataProvider parameter metadata provider (must not be {@literal null})
	 */
	public R2dbcQueryCreator(PartTree tree, ReactiveDataAccessStrategy dataAccessStrategy,
			RelationalEntityMetadata<?> entityMetadata, ParameterMetadataProvider parameterMetadataProvider) {
		super(tree);
		this.tree = tree;

		Assert.notNull(dataAccessStrategy, "Data access strategy must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");
		Assert.notNull(parameterMetadataProvider, "Parameter metadata provider must not be null");

		this.dataAccessStrategy = dataAccessStrategy;
		this.entityMetadata = entityMetadata;
		this.criteriaFactory = new CriteriaFactory(parameterMetadataProvider);
	}

	/**
	 * Creates {@link Criteria} for the given method name part.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param iterator iterator over query parameter values
	 * @return new instance of {@link Criteria}
	 */
	@Override
	protected Criteria create(Part part, Iterator<Object> iterator) {
		return criteriaFactory.createCriteria(part);
	}

	/**
	 * Combines the given {@link Criteria} with the new one created for the given method name part using {@code AND}.
	 *
	 * @param part method name part (must not be {@literal null})
	 * @param base {@link Criteria} to be combined (must not be {@literal null})
	 * @param iterator iterator over query parameter values
	 * @return {@link Criteria} combination
	 */
	@Override
	protected Criteria and(Part part, Criteria base, Iterator<Object> iterator) {
		return base.and(criteriaFactory.createCriteria(part));
	}

	/**
	 * Combines two {@link Criteria}s using {@code OR}.
	 *
	 * @param base {@link Criteria} to be combined (must not be {@literal null})
	 * @param criteria another {@link Criteria} to be combined (must not be {@literal null})
	 * @return {@link Criteria} combination
	 */
	@Override
	protected Criteria or(Criteria base, Criteria criteria) {
		return base.or(criteria);
	}

	/**
	 * Creates {@link PreparedOperation} applying the given {@link Criteria} and {@link Sort} definition.
	 *
	 * @param criteria {@link Criteria} to be applied to query
	 * @param sort sort option to be applied to query (must not be {@literal null})
	 * @return instance of {@link PreparedOperation}
	 */
	@Override
	protected PreparedOperation<?> complete(Criteria criteria, Sort sort) {
		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityMetadata.getJavaType());
		StatementMapper.SelectSpec selectSpec = statementMapper.createSelect(entityMetadata.getTableName())
				.withProjection(getSelectProjection());

		if (tree.isExistsProjection()) {
			selectSpec = selectSpec.limit(1);
		} else if (tree.isLimiting()) {
			selectSpec = selectSpec.limit(tree.getMaxResults());
		}

		if (criteria != null) {
			selectSpec = selectSpec.withCriteria(criteria);
		}

		if (sort.isSorted()) {
			selectSpec = selectSpec.withSort(getSort(sort));
		}

		return statementMapper.getMappedObject(selectSpec);
	}

	private SqlIdentifier[] getSelectProjection() {
		List<SqlIdentifier> columnNames;
		if (tree.isExistsProjection()) {
			columnNames = dataAccessStrategy.getIdentifierColumns(entityMetadata.getJavaType());
		} else {
			columnNames = dataAccessStrategy.getAllColumns(entityMetadata.getJavaType());
		}
		return columnNames.toArray(new SqlIdentifier[0]);
	}

	private Sort getSort(Sort sort) {
		RelationalPersistentEntity<?> tableEntity = entityMetadata.getTableEntity();
		List<Sort.Order> orders = sort.get().map(order -> {
			RelationalPersistentProperty property = tableEntity.getRequiredPersistentProperty(order.getProperty());
			String columnName = dataAccessStrategy.toSql(property.getColumnName());
			String orderProperty = entityMetadata.getTableName() + "." + columnName;
			// TODO: org.springframework.data.relational.core.sql.render.OrderByClauseVisitor from
			//  spring-data-relational does not prepend column name with table name. It makes sense to render
			//  column names uniformly.
			return order.isAscending() ? Sort.Order.asc(orderProperty) : Sort.Order.desc(orderProperty);
		}).collect(Collectors.toList());
		return Sort.by(orders);
	}
}
