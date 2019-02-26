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
package org.springframework.data.r2dbc.support;

import java.util.OptionalLong;

import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.r2dbc.dialect.LimitClause;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Utility class to assist with SQL rendering. Mainly for internal use within the framework.
 *
 * @author Mark Paluch
 */
public abstract class StatementRenderUtil {

	/**
	 * Render {@link Select} to SQL considering {@link Dialect} specifics.
	 *
	 * @param select must not be {@literal null}.
	 * @param limit must not be {@literal null}.
	 * @param offset must not be {@literal null}.
	 * @param dialect must not be {@literal null}.
	 * @return the rendered SQL statement.
	 */
	public static String render(Select select, OptionalLong limit, OptionalLong offset, Dialect dialect) {

		String sql = SqlRenderer.render(select);

		// TODO: Replace with proper {@link Dialect} rendering for limit/offset.
		if (limit.isPresent()) {

			LimitClause limitClause = dialect.limit();

			String clause;
			if (offset.isPresent()) {
				clause = limitClause.getClause(limit.getAsLong(), offset.getAsLong());
			} else {
				clause = limitClause.getClause(limit.getAsLong());
			}

			return sql + " " + clause;
		}

		return sql;
	}

	private StatementRenderUtil() {}

}
