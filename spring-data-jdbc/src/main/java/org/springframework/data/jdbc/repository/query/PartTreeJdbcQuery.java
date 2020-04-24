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

import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.repository.query.RelationalEntityMetadata;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * An {@link AbstractJdbcQuery} implementation based on a {@link PartTree}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class PartTreeJdbcQuery extends AbstractJdbcQuery {

	private final Parameters<?, ?> parameters;
	private final Dialect dialect;
	private final JdbcConverter converter;
	private final PartTree tree;
	private final JdbcQueryExecution<?> execution;

	/**
	 * Creates a new {@link PartTreeJdbcQuery}.
	 * 
	 * @param queryMethod must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param rowMapper must not be {@literal null}.
	 */
	public PartTreeJdbcQuery(JdbcQueryMethod queryMethod, Dialect dialect, JdbcConverter converter,
			NamedParameterJdbcOperations operations, RowMapper<Object> rowMapper) {

		super(queryMethod, operations, rowMapper);

		Assert.notNull(queryMethod, "JdbcQueryMethod must not be null");
		Assert.notNull(dialect, "Dialect must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");

		this.parameters = queryMethod.getParameters();
		this.dialect = dialect;
		this.converter = converter;

		this.tree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
		JdbcQueryCreator.validate(this.tree, this.parameters, this.converter.getMappingContext());

		this.execution = getQueryExecution(queryMethod, null, rowMapper);
	}

	private Sort getDynamicSort(RelationalParameterAccessor accessor) {
		return parameters.potentiallySortsDynamically() ? accessor.getSort() : Sort.unsorted();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] values) {

		RelationalParametersParameterAccessor accessor = new RelationalParametersParameterAccessor(getQueryMethod(),
				values);

		ParametrizedQuery query = createQuery(accessor);
		return this.execution.execute(query.getQuery(), query.getParameterSource());
	}

	protected ParametrizedQuery createQuery(RelationalParametersParameterAccessor accessor) {

		RelationalEntityMetadata<?> entityMetadata = getQueryMethod().getEntityInformation();
		JdbcQueryCreator queryCreator = new JdbcQueryCreator(tree, converter, dialect, entityMetadata, accessor);
		return queryCreator.createQuery(getDynamicSort(accessor));
	}
}
