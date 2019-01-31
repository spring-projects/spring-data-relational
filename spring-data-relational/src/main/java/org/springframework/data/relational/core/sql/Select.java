/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import java.util.OptionalLong;

/**
 * AST for a {@code SELECT} statement.
 * Visiting order:
 * <ol>
 * <li>Self</li>
 * <li>{@link Column SELECT columns} </li>
 * <li>{@link Table FROM tables} clause</li>
 * <li>{@link Join JOINs}</li>
 * <li>{@link Condition WHERE} condition</li>
 * <li>{@link OrderByField ORDER BY fields}</li>
 * </ol>
 *
 * @author Mark Paluch
 * @see StatementBuilder
 * @see SelectBuilder
 * @see SQL
 */
public interface Select extends Segment, Visitable {

	/**
	 * Creates a new {@link SelectBuilder}.
	 *
	 * @return a new {@link SelectBuilder}.
	 */
	static SelectBuilder builder() {
		return new DefaultSelectBuilder();
	}

	/**
	 * Optional limit. Used for limit/offset paging.
	 *
	 * @return
	 */
	OptionalLong getLimit();

	/**
	 * Optional offset. Used for limit/offset paging.
	 *
	 * @return
	 */
	OptionalLong getOffset();

	/**
	 * Flag if this select is to return distinct rows.
	 *
	 * @return
	 */
	boolean isDistinct();
}
