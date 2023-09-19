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

import java.util.Collections;
import java.util.Optional;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * {@link DelegatingDataAccessStrategy} applying Single Query Loading if the underlying aggregate type allows Single
 * Query Loading.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 3.2
 */
class SingleQueryFallbackDataAccessStrategy extends DelegatingDataAccessStrategy {

	private final SqlGeneratorSource sqlGeneratorSource;
	private final SingleQueryDataAccessStrategy singleSelectDelegate;
	private final JdbcConverter converter;

	public SingleQueryFallbackDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, JdbcConverter converter,
			NamedParameterJdbcOperations operations, DataAccessStrategy fallback) {

		super(fallback);

		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.converter = converter;

		this.singleSelectDelegate = new SingleQueryDataAccessStrategy(sqlGeneratorSource.getDialect(), converter,
				operations);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		if (isSingleSelectQuerySupported(domainType)) {
			return singleSelectDelegate.findById(id, domainType);
		}

		return super.findById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		if (isSingleSelectQuerySupported(domainType)) {
			return singleSelectDelegate.findAll(domainType);
		}

		return super.findAll(domainType);
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}

		if (isSingleSelectQuerySupported(domainType)) {
			return singleSelectDelegate.findAllById(ids, domainType);
		}

		return super.findAllById(ids, domainType);
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> domainType) {

		if (isSingleSelectQuerySupported(domainType) && isSingleSelectQuerySupported(query)) {
			return singleSelectDelegate.findOne(query, domainType);
		}

		return super.findOne(query, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType) {

		if (isSingleSelectQuerySupported(domainType) && isSingleSelectQuerySupported(query)) {
			return singleSelectDelegate.findAll(query, domainType);
		}

		return super.findAll(query, domainType);
	}

	private static boolean isSingleSelectQuerySupported(Query query) {
		return !query.isSorted() && !query.isLimited();
	}

	private boolean isSingleSelectQuerySupported(Class<?> entityType) {

		return converter.getMappingContext().isSingleQueryLoadingEnabled()
				&& sqlGeneratorSource.getDialect().supportsSingleQueryLoading()//
				&& entityQualifiesForSingleQueryLoading(entityType);
	}

	private boolean entityQualifiesForSingleQueryLoading(Class<?> entityType) {

		for (PersistentPropertyPath<RelationalPersistentProperty> path : converter.getMappingContext()
				.findPersistentPropertyPaths(entityType, __ -> true)) {
			RelationalPersistentProperty property = path.getLeafProperty();
			if (property.isEntity()) {

				// single references are currently not supported
				if (!(property.isMap() || property.isCollectionLike())) {
					return false;
				}

				// embedded entities are currently not supported
				if (property.isEmbedded()) {
					return false;
				}

				// nested references are currently not supported
				if (path.getLength() > 1) {
					return false;
				}
			}
		}
		return true;

	}
}
