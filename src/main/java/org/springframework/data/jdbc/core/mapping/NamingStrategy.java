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

/**
 * Interface and default implementation of a naming strategy. Defaults to no schema, table name based on {@link Class}
 * and column name based on {@link JdbcPersistentProperty}.
 * <p>
 * NOTE: Can also be used as an adapter. Create a lambda or an anonymous subclass and override any settings to implement
 * a different strategy on the fly.
 * 
 * @author Greg Turnquist
 * @author Michael Simons
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @since 1.0
 */
public interface NamingStrategy {

	/**
	 * Empty implementation of the interface utilizing only the default implementation.
	 * <p>
	 * Using this avoids creating essentially the same class over and over again.
	 */
	NamingStrategy INSTANCE = new NamingStrategy() {};

	/**
	 * Defaults to no schema.
	 *
	 * @return Empty String representing no schema
	 */
	default String getSchema() {
		return "";
	}

	/**
	 * Defaults to returning the given type's simple name.
	 */
	default String getTableName(Class<?> type) {
		return type.getSimpleName();
	}

	/**
	 * Defaults to return the given {@link JdbcPersistentProperty}'s name;
	 */
	default String getColumnName(JdbcPersistentProperty property) {
		return property.getName();
	}

	default String getQualifiedTableName(Class<?> type) {
		return this.getSchema() + (this.getSchema().equals("") ? "" : ".") + this.getTableName(type);
	}

	/**
	 * For a reference A -&gt; B this is the name in the table for B which references A.
	 *
	 * @param property The property who's column name in the owner table is required
	 * @return a column name. Must not be {@code null}.
	 */
	default String getReverseColumnName(JdbcPersistentProperty property) {
		return property.getOwner().getTableName();
	}

	/**
	 * For a map valued reference A -> Map&gt;X,B&lt; this is the name of the column in the table for B holding the key of
	 * the map.
	 * 
	 * @return name of the key column. Must not be {@code null}.
	 */
	default String getKeyColumn(JdbcPersistentProperty property) {
		return getReverseColumnName(property) + "_key";
	}
}
