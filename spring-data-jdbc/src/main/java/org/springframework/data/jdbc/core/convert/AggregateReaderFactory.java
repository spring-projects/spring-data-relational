/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Creates {@link AggregateReader} instances.
 * 
 * @since 3.2
 * @author Jens Schauder
 */
class AggregateReaderFactory {

	private final RelationalMappingContext mappingContext;
	private final Dialect dialect;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations jdbcTemplate;

	public AggregateReaderFactory(RelationalMappingContext mappingContext, Dialect dialect, JdbcConverter converter,
			NamedParameterJdbcOperations jdbcTemplate) {

		this.mappingContext = mappingContext;
		this.dialect = dialect;
		this.converter = converter;
		this.jdbcTemplate = jdbcTemplate;
	}

	<T> AggregateReader<T> createAggregateReaderFor(RelationalPersistentEntity<T> entity) {
		return new AggregateReader<>(mappingContext, dialect, converter, jdbcTemplate, entity);
	}
}
