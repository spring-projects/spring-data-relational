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

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.PreparedOperation;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalQueryCreator;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Implementation of {@link AbstractQueryCreator} that creates {@link PreparedOperation} from a {@link PartTree}.
 *
 * @author Roman Chigvintsev
 * @author Mark Paluch
 * @author Mingyuan Wu
 * @since 1.1
 */
public class R2dbcQueryCreator extends RelationalQueryCreator<PreparedOperation<?>> {

	private final PartTree tree;
	private final RelationalParameterAccessor accessor;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final RelationalEntityMetadata<?> entityMetadata;

	/**
	 * Creates new instance of this class with the given {@link PartTree}, {@link ReactiveDataAccessStrategy},
	 * {@link RelationalEntityMetadata} and {@link RelationalParameterAccessor}.
	 *
	 * @param tree part tree, must not be {@literal null}.
	 * @param dataAccessStrategy data access strategy, must not be {@literal null}.
	 * @param entityMetadata relational entity metadata, must not be {@literal null}.
	 * @param accessor parameter metadata provider, must not be {@literal null}.
	 */
	public R2dbcQueryCreator(PartTree tree, ReactiveDataAccessStrategy dataAccessStrategy,
			RelationalEntityMetadata<?> entityMetadata, RelationalParameterAccessor accessor) {
		super(tree, accessor);

		Assert.notNull(dataAccessStrategy, "Data access strategy must not be null");
		Assert.notNull(entityMetadata, "Relational entity metadata must not be null");

		this.tree = tree;
		this.accessor = accessor;

		this.dataAccessStrategy = dataAccessStrategy;
		this.entityMetadata = entityMetadata;
	}

	/**
	 * Creates {@link PreparedOperation} applying the given {@link Criteria} and {@link Sort} definition.
	 *
	 * @param criteria {@link Criteria} to be applied to query
	 * @param sort sort option to be applied to query, must not be {@literal null}.
	 * @return instance of {@link PreparedOperation}
	 */
	@Override
	protected PreparedOperation<?> complete(Criteria criteria, Sort sort) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityMetadata.getJavaType());
		if(tree.isDelete()){
			StatementMapper.DeleteSpec deleteSpec = statementMapper.createDelete(entityMetadata.getTableName()).withCriteria(criteria);
			return statementMapper.getMappedObject(deleteSpec);
		}
		StatementMapper.SelectSpec selectSpec = statementMapper.createSelect(entityMetadata.getTableName())
				.withProjection(getSelectProjection());

		if (tree.isExistsProjection()) {
			selectSpec = selectSpec.limit(1);
		} else if (tree.isLimiting()) {
			selectSpec = selectSpec.limit(tree.getMaxResults());
		}

		Pageable pageable = accessor.getPageable();
		if (pageable.isPaged()) {
			selectSpec = selectSpec.limit(pageable.getPageSize()).offset(pageable.getOffset());
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
			return order.isAscending() ? Sort.Order.asc(property.getName()) : Sort.Order.desc(property.getName());
		}).collect(Collectors.toList());

		return Sort.by(orders);
	}
}
