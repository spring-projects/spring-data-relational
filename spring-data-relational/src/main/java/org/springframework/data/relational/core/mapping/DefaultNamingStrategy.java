/*
 * Copyright 2022-2024 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * The default naming strategy used by Spring Data Relational. Names are in {@code SNAKE_CASE}.
 *
 * @author Jens Schauder
 * @since 3.0
 */
public class DefaultNamingStrategy implements NamingStrategy {

	/**
	 * Static immutable instance of the class. It is made immutable by letting
	 * {@link #setForeignKeyNaming(ForeignKeyNaming)} throw an exception.
	 * <p>
	 * Using this avoids creating essentially the same class over and over again.
	 */
	public static NamingStrategy INSTANCE = new DefaultNamingStrategy() {
		@Override
		public void setForeignKeyNaming(ForeignKeyNaming foreignKeyNaming) {
			throw new UnsupportedOperationException("Cannot update immutable DefaultNamingStrategy");
		}
	};

	private ForeignKeyNaming foreignKeyNaming = ForeignKeyNaming.APPLY_RENAMING;

	public void setForeignKeyNaming(ForeignKeyNaming foreignKeyNaming) {

		Assert.notNull(foreignKeyNaming, "foreignKeyNaming must not be null");

		this.foreignKeyNaming = foreignKeyNaming;
	}

	@Override
	public String getReverseColumnName(RelationalPersistentProperty property) {

		return getColumnNameReferencing(property.getOwner());
	}
	
	@Override
	public String getReverseColumnName(RelationalPersistentEntity<?> parent) {
		return getColumnNameReferencing(parent);
	}

	private String getColumnNameReferencing(RelationalPersistentEntity<?> entity) {

		if (foreignKeyNaming == ForeignKeyNaming.IGNORE_RENAMING) {
			return getTableName(entity.getType());
		}

		return entity.getTableName().getReference();
	}
}
