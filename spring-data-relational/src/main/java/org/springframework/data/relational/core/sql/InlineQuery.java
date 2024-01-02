/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

/**
 * Represents a inline query within a SQL statement. Typically, used in {@code FROM} or {@code JOIN} clauses.
 * <p>
 * Renders to: {@code (&gt;SELECT&lt;) AS &gt;ALIAS&lt;} in a from or join clause, and to {@code &gt;ALIAS&lt;} when
 * used in an expression.
 * <p>
 * Note that this does not implement {@link Aliased} because the Alias is not optional but required and therefore more
 * like a name although the SQL term is "alias".
 *
 * @author Jens Schauder
 * @since 2.3
 */
public class InlineQuery extends Subselect implements TableLike {

	private final SqlIdentifier alias;

	InlineQuery(Select select, SqlIdentifier alias) {

		super(select);

		this.alias = alias;
	}

	/**
	 * Creates a new {@link InlineQuery} using an {@code alias}.
	 *
	 * @param select must not be {@literal null}.
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link InlineQuery} using the {@code alias}.
	 */
	public static InlineQuery create(Select select, SqlIdentifier alias) {

		Assert.notNull(select, "Select must not be null");
		Assert.notNull(alias, "Alias must not be null or empty");

		return new InlineQuery(select, alias);
	}

	/**
	 * Creates a new {@link InlineQuery} using an {@code alias}.
	 *
	 * @param select must not be {@literal null} or empty.
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@link InlineQuery} using the {@code alias}.
	 */
	public static InlineQuery create(Select select, String alias) {
		return create(select, SqlIdentifier.unquoted(alias));
	}

	@Override
	public SqlIdentifier getName() {
		return alias;
	}

	/**
	 * @return the table name as it is used in references. This can be the actual {@link #getName() name} or an
	 *         {@link Aliased#getAlias() alias}.
	 */
	@Override
	public SqlIdentifier getReferenceName() {
		return alias;
	}
}
