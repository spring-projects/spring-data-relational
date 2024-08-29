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
package org.springframework.data.relational.core.mapping;

import org.springframework.data.util.ParsingUtils;
import org.springframework.util.Assert;

/**
 * Interface and default implementation of a naming strategy. Defaults to no schema, table name based on {@link Class}
 * and column name based on {@link RelationalPersistentProperty} with name parts of both separated by '_'.
 * <p>
 * NOTE: Can also be used as an adapter. Create a lambda or an anonymous subclass and override any settings to implement
 * a different strategy on the fly.
 *
 * @author Greg Turnquist
 * @author Michael Simons
 * @author Kazuki Shimizu
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Kurt Niemi
 */
public interface NamingStrategy {

	/**
	 * Defaults to no schema.
	 *
	 * @return Empty String representing no schema
	 */
	default String getSchema() {
		return "";
	}

	/**
	 * The name of the table to be used for persisting entities having the type passed as an argument. The default
	 * implementation takes the {@code type.getSimpleName()} and separates camel case parts with '_'.
	 */
	default String getTableName(Class<?> type) {

		Assert.notNull(type, "Type must not be null");

		return ParsingUtils.reconcatenateCamelCase(type.getSimpleName(), "_");
	}

	/**
	 * Defaults to return the given {@link RelationalPersistentProperty}'s name with the parts of a camel case name
	 * separated by '_';
	 */
	default String getColumnName(RelationalPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		return ParsingUtils.reconcatenateCamelCase(property.getName(), "_");
	}

	/**
	 * For a reference A -&gt; B this is the name in the table for B which references A.
	 *
	 * @param property The property whose column name in the owner table is required
	 * @return a column name. Must not be {@code null}.
	 */
	default String getReverseColumnName(RelationalPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		return property.getOwner().getTableName().getReference();
	}

	default String getReverseColumnName(RelationalPersistentEntity<?> owner) {
		return getTableName(owner.getType());
	}

	/**
	 * For a map valued reference A -> Map&gt;X,B&lt; this is the name of the column in the table for B holding the key of
	 * the map.
	 *
	 * @return name of the key column. Must not be {@code null}.
	 */
	default String getKeyColumn(RelationalPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		return getReverseColumnName(property) + "_key";
	}

}
