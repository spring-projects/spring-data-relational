/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.data.util.ParsingUtils;

/**
 * The delimiter character implementation of {@link NamingStrategy} with no schema, table based on {@link Class} and
 * column name based on {@link JdbcPersistentProperty}. The default delimiter is '_'.
 * This indicate that the default behavior is snake case.
 *
 * @author Kazuki Shimizu
 */
public class DelimiterNamingStrategy extends DefaultNamingStrategy {

	private final String delimiter;

	/**
	 * Construct a instance with '_' as delimiter.
	 * <p>
	 * This indicate that default is snake case.
	 */
	public DelimiterNamingStrategy() {
		this("_");
	}

	/**
	 * Construct a instance with specified delimiter.
	 *
	 * @param delimiter a delimiter character
	 */
	public DelimiterNamingStrategy(String delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * Look up the {@link Class}'s simple name after converting to separated word using with {@code delimiter}.
	 */
	@Override
	public String getTableName(Class<?> type) {
		return ParsingUtils.reconcatenateCamelCase(super.getTableName(type), delimiter);
	}

	/**
	 * Look up the {@link JdbcPersistentProperty}'s name after converting to separated word using with {@code delimiter}.
	 */
	@Override
	public String getColumnName(JdbcPersistentProperty property) {
		return ParsingUtils.reconcatenateCamelCase(super.getColumnName(property), delimiter);
	}

	/**
	 * Return the value that adding {@code delimiter} + 'key' for returned value of {@link #getReverseColumnName}.
	 */
	@Override
	public String getKeyColumn(JdbcPersistentProperty property) {
		return getReverseColumnName(property) + delimiter + "key";
	}


}
