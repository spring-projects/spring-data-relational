/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.util.Lazy;
import org.springframework.util.Assert;
import org.springframework.util.ConcurrentReferenceHashMap;

/**
 * A {@link NamingStrategy} to cache the results of the target one.
 *
 * @author Oliver Drotbohm
 * @since 1.1
 */
class CachingNamingStrategy implements NamingStrategy {

	private final NamingStrategy delegate;

	private final Map<RelationalPersistentProperty, String> columnNames = new ConcurrentHashMap<>();
	private final Map<RelationalPersistentProperty, String> keyColumns = new ConcurrentHashMap<>();
	private final Map<Class<?>, String> qualifiedTableNames = new ConcurrentReferenceHashMap<>();
	private final Map<Class<?>, String> tableNames = new ConcurrentReferenceHashMap<>();

	private final Lazy<String> schema;

	/**
	 * Creates a new {@link CachingNamingStrategy} with the given delegate {@link NamingStrategy}.
	 *
	 * @param delegate must not be {@literal null}.
	 */
	public CachingNamingStrategy(NamingStrategy delegate) {

		Assert.notNull(delegate, "Delegate must not be null!");

		this.delegate = delegate;
		this.schema = Lazy.of(delegate::getSchema);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getKeyColumn(org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public String getKeyColumn(RelationalPersistentProperty property) {
		return keyColumns.computeIfAbsent(property, delegate::getKeyColumn);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getQualifiedTableName(java.lang.Class)
	 */
	@Override
	public String getQualifiedTableName(Class<?> type) {
		return qualifiedTableNames.computeIfAbsent(type, delegate::getQualifiedTableName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> type) {
		return tableNames.computeIfAbsent(type, delegate::getTableName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getReverseColumnName(org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension)
	 */
	@Override
	public String getReverseColumnName(PersistentPropertyPathExtension path) {
		return delegate.getReverseColumnName(path);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getReverseColumnName(org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public String getReverseColumnName(RelationalPersistentProperty property) {
		return delegate.getReverseColumnName(property);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getSchema()
	 */
	@Override
	public String getSchema() {
		return schema.get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.mapping.NamingStrategy#getColumnName(org.springframework.data.relational.core.mapping.RelationalPersistentProperty)
	 */
	@Override
	public String getColumnName(RelationalPersistentProperty property) {
		return columnNames.computeIfAbsent(property, delegate::getColumnName);
	}
}
