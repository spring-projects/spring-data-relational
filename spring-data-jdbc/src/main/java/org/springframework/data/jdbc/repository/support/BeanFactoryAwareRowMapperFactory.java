/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * This {@link RowMapperFactory} implementation extends the {@link DefaultRowMapperFactory} by adding the capabilities
 * to load {@link RowMapper} or {@link ResultSetExtractor} beans by their names in {@link BeanFactory}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mikhail Polivakha
 */
@SuppressWarnings("unchecked")
public class BeanFactoryAwareRowMapperFactory extends DefaultRowMapperFactory {

	private final BeanFactory beanFactory;

	/**
	 * Create a {@code BeanFactoryAwareRowMapperFactory} instance using the given {@link BeanFactory},
	 * {@link JdbcAggregateOperations} and {@link QueryMappingConfiguration}.
	 *
	 * @param beanFactory
	 * @param operations
	 * @param queryMappingConfiguration
	 */
	public BeanFactoryAwareRowMapperFactory(BeanFactory beanFactory, JdbcAggregateOperations operations,
			QueryMappingConfiguration queryMappingConfiguration) {

		super(operations, queryMappingConfiguration);

		this.beanFactory = beanFactory;
	}

	@Override
	public RowMapper<Object> getRowMapper(String reference) {
		return beanFactory.getBean(reference, RowMapper.class);
	}

	@Override
	public ResultSetExtractor<Object> getResultSetExtractor(String reference) {
		return beanFactory.getBean(reference, ResultSetExtractor.class);
	}

}
