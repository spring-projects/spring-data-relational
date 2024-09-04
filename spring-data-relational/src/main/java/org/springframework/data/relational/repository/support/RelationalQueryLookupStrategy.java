/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.data.relational.repository.support;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.util.Assert;

/**
 * Base class for R2DBC and JDBC {@link QueryLookupStrategy} implementations.
 *
 * @author Jens Schauder
 * @since 3.4
 */
public abstract class RelationalQueryLookupStrategy implements QueryLookupStrategy {

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context;
	private final Dialect dialect;

	protected RelationalQueryLookupStrategy(
			MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> context,
			Dialect dialect) {

		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(dialect, "Dialect must not be null");

		this.context = context;
		this.dialect = dialect;
	}

	public MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> getMappingContext() {
		return context;
	}

	public Dialect getDialect() {
		return dialect;
	}

	protected String evaluateTableExpressions(RepositoryMetadata repositoryMetadata, String queryString) {

		TableNameQueryPreprocessor preprocessor = new TableNameQueryPreprocessor(
				context.getRequiredPersistentEntity(repositoryMetadata.getDomainType()), dialect);

		return preprocessor.transform(queryString);
	}

}
