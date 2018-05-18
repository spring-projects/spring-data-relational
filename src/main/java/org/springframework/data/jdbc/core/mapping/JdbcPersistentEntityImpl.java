/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.mapping;

import java.util.Optional;

import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;

/**
 * Meta data a repository might need for implementing persistence operations for instances of type {@code T}
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @since 1.0
 */
class JdbcPersistentEntityImpl<T> extends BasicPersistentEntity<T, JdbcPersistentProperty>
		implements JdbcPersistentEntity<T> {

	private final NamingStrategy namingStrategy;
	private final Lazy<Optional<String>> tableName;

	/**
	 * Creates a new {@link JdbcPersistentEntityImpl} for the given {@link TypeInformation}.
	 *
	 * @param information must not be {@literal null}.
	 */
	JdbcPersistentEntityImpl(TypeInformation<T> information, NamingStrategy namingStrategy) {

		super(information);

		this.namingStrategy = namingStrategy;
		this.tableName = Lazy.of(() -> Optional.ofNullable(findAnnotation(Table.class)).map(Table::value));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity#getTableName()
	 */
	@Override
	public String getTableName() {
		return tableName.get().orElseGet(() -> namingStrategy.getQualifiedTableName(getType()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.mapping.model.JdbcPersistentEntity#getIdColumn()
	 */
	@Override
	public String getIdColumn() {
		return this.namingStrategy.getColumnName(getRequiredIdProperty());
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("JdbcPersistentEntityImpl<%s>", getType());
	}
}
