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
package org.springframework.data.jdbc.mapping.model;

import org.springframework.core.annotation.AnnotatedElementUtils;

import java.util.Optional;

/**
 * Interface and default implementation of a naming strategy. Defaults to no schema, table name based on {@link Class}
 * and column name based on {@link JdbcPersistentProperty}.
 * <p>
 * NOTE: Can also be used as an adapter. Create a lambda or an anonymous subclass and override any settings to implement
 * a different strategy on the fly.
 * 
 * @author Greg Turnquist
 * @author Michael Simons
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
	 * Look up the {@link Class}'s simple name or {@link Table#value()}.
	 */
	default String getTableName(Class<?> type) {
		Table table = AnnotatedElementUtils.findMergedAnnotation(type, Table.class);
		return Optional.ofNullable(table)//
			.map(Table::value)//
			.orElse(type.getSimpleName());
	}

	/**
	 * Look up the {@link JdbcPersistentProperty}'s name or {@link Column#value()}.
	 */
	default String getColumnName(JdbcPersistentProperty property) {
		Column column = property.findAnnotation(Column.class);
		return Optional.ofNullable(column)//
			.map(Column::value)//
			.orElse(property.getName());
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
