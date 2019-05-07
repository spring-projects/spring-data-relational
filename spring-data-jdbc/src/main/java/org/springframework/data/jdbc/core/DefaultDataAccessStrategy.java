/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.SqlGeneratorSource;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * The default {@link DataAccessStrategy} is to generate SQL statements based on meta data from the entity.
 *
 * @author Jens Schauder
 * @deprecated since 1.1, use {@link org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy} instead.
 */
@Deprecated
public class DefaultDataAccessStrategy extends org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy {

	/**
	 * Creates a {@link org.springframework.data.jdbc.core.convert.DefaultDataAccessStrategy} which references it self for
	 * resolution of recursive data accesses. Only suitable if this is the only access strategy in use.
	 *
	 * @param sqlGeneratorSource must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public DefaultDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations) {
		super(sqlGeneratorSource, context, converter, operations);
	}

}
