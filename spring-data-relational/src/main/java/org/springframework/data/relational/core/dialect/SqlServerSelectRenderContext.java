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
package org.springframework.data.relational.core.dialect;

import java.util.function.Function;

import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;

/**
 * SQL-Server specific {@link SelectRenderContext}. Summary of SQL-specifics:
 * <ul>
 * <li>Appends a synthetic ROW_NUMBER when using pagination and the query does not specify ordering</li>
 * <li>Append synthetic ordering if query uses pagination and the query does not specify ordering</li>
 * </ul>
 *
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 */
public class SqlServerSelectRenderContext implements SelectRenderContext {

	private static final String SYNTHETIC_ORDER_BY_FIELD = "__relational_row_number__";

	private static final String SYNTHETIC_SELECT_LIST = ", ROW_NUMBER() over (ORDER BY (SELECT 1)) AS "
			+ SYNTHETIC_ORDER_BY_FIELD;

	private final Function<Select, CharSequence> afterFromTable;
	private final Function<Select, CharSequence> afterOrderBy;

	/**
	 * Creates a new {@link SqlServerSelectRenderContext}.
	 *
	 * @param afterFromTable the delegate {@code afterFromTable} function.
	 * @param afterOrderBy the delegate {@code afterOrderBy} function.
	 */
	protected SqlServerSelectRenderContext(Function<Select, CharSequence> afterFromTable,
			Function<Select, CharSequence> afterOrderBy) {

		this.afterFromTable = afterFromTable;
		this.afterOrderBy = afterOrderBy;
	}

	@Override
	public Function<Select, ? extends CharSequence> afterSelectList() {

		return select -> {

			if (usesPagination(select) && select.getOrderBy().isEmpty()) {
				return SYNTHETIC_SELECT_LIST;
			}

			return "";
		};
	}

	@Override
	public Function<Select, ? extends CharSequence> afterFromTable() {

		return afterFromTable;
	}

	@Override
	public Function<Select, ? extends CharSequence> afterOrderBy(boolean hasOrderBy) {

		if (hasOrderBy) {
			return afterOrderBy;
		}

		return select -> {

			StringBuilder builder = new StringBuilder();

			if (usesPagination(select)) {
				builder.append(" ORDER BY " + SYNTHETIC_ORDER_BY_FIELD);
			}

			builder.append(afterOrderBy.apply(select));

			return builder;
		};
	}

	private static boolean usesPagination(Select select) {
		return select.getOffset().isPresent() || select.getLimit().isPresent();
	}
}
