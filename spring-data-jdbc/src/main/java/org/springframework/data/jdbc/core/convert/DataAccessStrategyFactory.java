/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * Factory to create a {@link DataAccessStrategy} based on the configuration of the provided components. Specifically,
 * this factory creates a {@link SingleQueryFallbackDataAccessStrategy} that falls back to
 * {@link DefaultDataAccessStrategy} if Single Query Loading is not supported. This factory encapsulates
 * {@link DataAccessStrategy} for consistent access strategy creation.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @since 3.2
 */
public class DataAccessStrategyFactory {

	private final SqlGeneratorSource sqlGeneratorSource;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations operations;
	private final SqlParametersFactory sqlParametersFactory;
	private final InsertStrategyFactory insertStrategyFactory;
	private final QueryMappingConfiguration queryMappingConfiguration;

	/**
	 * Creates a new {@link DataAccessStrategyFactory}.
	 *
	 * @param sqlGeneratorSource must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param sqlParametersFactory must not be {@literal null}.
	 * @param insertStrategyFactory must not be {@literal null}.
	 */
	public DataAccessStrategyFactory(SqlGeneratorSource sqlGeneratorSource, JdbcConverter converter,
			NamedParameterJdbcOperations operations, SqlParametersFactory sqlParametersFactory,
			InsertStrategyFactory insertStrategyFactory, QueryMappingConfiguration queryMappingConfiguration) {

		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");
		Assert.notNull(sqlParametersFactory, "SqlParametersFactory must not be null");
		Assert.notNull(insertStrategyFactory, "InsertStrategyFactory must not be null");

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.converter = converter;
		this.operations = operations;
		this.sqlParametersFactory = sqlParametersFactory;
		this.insertStrategyFactory = insertStrategyFactory;
		this.queryMappingConfiguration = queryMappingConfiguration;
	}

	/**
	 * Creates a new {@link DataAccessStrategy}.
	 *
	 * @return a new {@link DataAccessStrategy}.
	 */
	public DataAccessStrategy create() {

		DefaultDataAccessStrategy defaultDataAccessStrategy = new DefaultDataAccessStrategy(sqlGeneratorSource,
				this.converter.getMappingContext(), this.converter, this.operations, sqlParametersFactory,
				insertStrategyFactory, queryMappingConfiguration);

		if (this.converter.getMappingContext().isSingleQueryLoadingEnabled()) {
			return new SingleQueryFallbackDataAccessStrategy(sqlGeneratorSource, converter, operations,
					defaultDataAccessStrategy);
		}

		return defaultDataAccessStrategy;
	}
}
