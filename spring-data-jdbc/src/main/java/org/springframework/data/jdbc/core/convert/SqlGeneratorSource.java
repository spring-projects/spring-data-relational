/*
 * Copyright 2017-2024 the original author or authors.
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

import java.util.Map;

import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;

/**
 * Provides {@link SqlGenerator}s per domain type. Instances get cached, so when asked multiple times for the same
 * domain type, the same generator will get returned.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Milan Milanov
 */
public class SqlGeneratorSource {

	private final Map<Class<?>, SqlGenerator> CACHE = new ConcurrentReferenceHashMap<>();
	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final Dialect dialect;

	public SqlGeneratorSource(RelationalMappingContext context, JdbcConverter converter, Dialect dialect) {

		Assert.notNull(context, "Context must not be null");
		Assert.notNull(converter, "Converter must not be null");
		Assert.notNull(dialect, "Dialect must not be null");

		this.context = context;
		this.converter = converter;
		this.dialect = dialect;
	}

	/**
	 * @return the {@link Dialect} used by the created {@link SqlGenerator} instances. Guaranteed to be not
	 *         {@literal null}.
	 */
	public Dialect getDialect() {
		return dialect;
	}

	SqlGenerator getSqlGenerator(Class<?> domainType) {

		return CACHE.computeIfAbsent(domainType,
				t -> new SqlGenerator(context, converter, context.getRequiredPersistentEntity(t), dialect));
	}
}
