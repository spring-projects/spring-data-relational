/*
 * Copyright 2022 the original author or authors.
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

import org.jetbrains.annotations.NotNull;
import org.springframework.util.Assert;

/**
 * The default naming strategy used by Spring Data Relational. Names are in SNAKE_CASE.
 *
 * @author Jens Schauder
 * @since 2.4
 */
public class DefaultNamingStrategy implements NamingStrategy {

	/**
	 * Since in most cases it doesn't make sense to have more than one {@link NamingStrategy} use of this instance is
	 * recommended.
	 */
	public static NamingStrategy INSTANCE = new DefaultNamingStrategy();

	private ForeignKeyNaming foreignKeyNaming = ForeignKeyNaming.IGNORE_RENAMING;

	@Override
	public void setForeignKeyNaming(ForeignKeyNaming foreignKeyNaming) {

		Assert.notNull(foreignKeyNaming, "foreignKeyNaming must not be null");

		this.foreignKeyNaming = foreignKeyNaming;
	}

	@Override
	public String getReverseColumnName(RelationalPersistentProperty property) {

		return getColumnNameReferencing(property.getOwner());
	}

	@Override
	public String getReverseColumnName(PersistentPropertyPathExtension path) {

		RelationalPersistentEntity<?> leafEntity = path.getIdDefiningParentPath().getLeafEntity();

		return getColumnNameReferencing(leafEntity);
	}

	private String getColumnNameReferencing(RelationalPersistentEntity<?> leafEntity) {

		Assert.state(leafEntity != null, "Leaf Entity must not be null.");

		if (foreignKeyNaming == ForeignKeyNaming.IGNORE_RENAMING) {
			return getTableName(leafEntity.getType());
		}

		return leafEntity.getSimpleTableName().getReference();
	}
}
