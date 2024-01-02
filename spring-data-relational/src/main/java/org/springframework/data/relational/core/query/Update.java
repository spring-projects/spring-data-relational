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
package org.springframework.data.relational.core.query;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Class to easily construct SQL update assignments.
 *
 * @author Mark Paluch
 * @author Oliver Drotbohm
 * @since 2.0
 */
public class Update {

	private static final Update EMPTY = new Update(Collections.emptyMap());

	private final Map<SqlIdentifier, Object> columnsToUpdate;

	private Update(Map<SqlIdentifier, Object> columnsToUpdate) {
		this.columnsToUpdate = columnsToUpdate;
	}

	/**
	 * Static factory method to create an {@link Update} from {@code assignments}.
	 *
	 * @param assignments must not be {@literal null}.
	 * @return
	 */
	public static Update from(Map<SqlIdentifier, Object> assignments) {
		return new Update(new LinkedHashMap<>(assignments));
	}

	/**
	 * Static factory method to create an {@link Update} using the provided column.
	 *
	 * @param column must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return
	 */
	public static Update update(String column, @Nullable Object value) {
		return EMPTY.set(column, value);
	}

	/**
	 * Update a column by assigning a value.
	 *
	 * @param column must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return
	 */
	public Update set(String column, @Nullable Object value) {

		Assert.hasText(column, "Column for update must not be null or blank");

		return addMultiFieldOperation(SqlIdentifier.unquoted(column), value);
	}

	/**
	 * Update a column by assigning a value.
	 *
	 * @param column must not be {@literal null}.
	 * @param value can be {@literal null}.
	 * @return
	 * @since 1.1
	 */
	public Update set(SqlIdentifier column, @Nullable Object value) {
		return addMultiFieldOperation(column, value);
	}

	/**
	 * Returns all assignments.
	 *
	 * @return
	 */
	public Map<SqlIdentifier, Object> getAssignments() {
		return Collections.unmodifiableMap(this.columnsToUpdate);
	}

	private Update addMultiFieldOperation(SqlIdentifier key, @Nullable Object value) {

		Assert.notNull(key, "Column for update must not be null");

		Map<SqlIdentifier, Object> updates = new LinkedHashMap<>(this.columnsToUpdate);
		updates.put(key, value);

		return new Update(updates);
	}

	@Override
	public String toString() {

		if (getAssignments().isEmpty()) {
			return "";
		}

		StringJoiner joiner = new StringJoiner(", ");

		getAssignments().forEach((column, o) -> {
			joiner.add(
					String.format("%s = %s", column.toSql(IdentifierProcessing.NONE), o instanceof Number ? o : "'" + o + "'"));
		});

		return "SET " + joiner;
	}
}
