/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function.query;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Class to easily construct SQL update assignments.
 *
 * @author Mark Paluch
 */
public class Update {

	private static final Update EMPTY = new Update(Collections.emptyMap());

	private final Map<String, Object> columnsToUpdate;

	private Update(Map<String, Object> columnsToUpdate) {
		this.columnsToUpdate = columnsToUpdate;
	}

	/**
	 * Static factory method to create an {@link Update} using the provided column.
	 *
	 * @param column
	 * @param value
	 * @return
	 */
	public static Update update(String column, @Nullable Object value) {
		return EMPTY.set(column, value);
	}

	/**
	 * Update a column by assigning a value.
	 *
	 * @param column
	 * @param value
	 * @return
	 */
	public Update set(String column, @Nullable Object value) {
		return addMultiFieldOperation(column, value);
	}

	private Update addMultiFieldOperation(String key, Object value) {

		Assert.hasText(key, "Column for update must not be null or blank");

		Map<String, Object> updates = new LinkedHashMap<>(this.columnsToUpdate);
		updates.put(key, value);

		return new Update(updates);
	}

	/**
	 * Performs the given action for each column-value tuple in this {@link Update object} until all entries have been
	 * processed or the action throws an exception.
	 *
	 * @param action must not be {@literal null}.
	 */
	void forEachColumn(BiConsumer<? super String, ? super Object> action) {
		this.columnsToUpdate.forEach(action);
	}

	public Map<String, Object> getAssignments() {
		return Collections.unmodifiableMap(this.columnsToUpdate);
	}
}
