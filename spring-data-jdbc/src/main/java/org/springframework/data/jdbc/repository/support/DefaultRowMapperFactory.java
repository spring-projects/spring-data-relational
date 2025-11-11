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
package org.springframework.data.jdbc.repository.support;

import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RowMapperFactory}. Honors the custom mappings defined in
 * {@link QueryMappingConfiguration}.
 * <p>
 * This implementation is not capable of loading the {@link RowMapper} or {@link ResultSetExtractor} by reference via
 * corresponding methods from {@link RowMapperFactory}.
 * <p>
 * Implementation is thread-safe.
 *
 * @author Mikhail Polivakha
 * @since 4.0
 */
public class DefaultRowMapperFactory implements RowMapperFactory {

	private final JdbcAggregateOperations operations;
	private final QueryMappingConfiguration queryMappingConfiguration;

	public DefaultRowMapperFactory(JdbcAggregateOperations operations,
			QueryMappingConfiguration queryMappingConfiguration) {

		Assert.notNull(operations, "JdbcAggregateOperations must not be null");
		Assert.notNull(queryMappingConfiguration, "QueryMappingConfiguration must not be null");

		this.operations = operations;
		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	@Override
	@SuppressWarnings("unchecked")
	public RowMapper<Object> create(Class<?> returnedObjectType) {

		RelationalPersistentEntity<?> persistentEntity = operations.getConverter().getMappingContext()
				.getPersistentEntity(returnedObjectType);

		if (persistentEntity == null) {

			return (RowMapper<Object>) SingleColumnRowMapper.newInstance(returnedObjectType,
					operations.getConverter().getConversionService());
		}

		return (RowMapper<Object>) determineDefaultMapper(returnedObjectType);
	}

	private RowMapper<?> determineDefaultMapper(Class<?> returnedObjectType) {

		RowMapper<?> configuredQueryMapper = queryMappingConfiguration.getRowMapper(returnedObjectType);

		if (configuredQueryMapper != null) {
			return configuredQueryMapper;
		}


		return operations.getRowMapper(returnedObjectType);
	}
}
