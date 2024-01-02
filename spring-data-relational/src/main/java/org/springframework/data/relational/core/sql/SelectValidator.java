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

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

/**
 * Validator for {@link Select} statements.
 * <p>
 * Validates that all {@link Column}s using a table qualifier have a table import from either the {@code FROM} or
 * {@code JOIN} clause.
 * </p>
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class SelectValidator extends AbstractImportValidator {

	private final Stack<Select> selects = new Stack<>();

	private int selectFieldCount;
	private Set<TableLike> requiredBySelect = new HashSet<>();
	private Set<TableLike> requiredByOrderBy = new HashSet<>();

	private Set<TableLike> join = new HashSet<>();

	/**
	 * Validates a {@link Select} statement.
	 *
	 * @param select the {@link Select} statement.
	 * @throws IllegalStateException if the statement is not valid.
	 */
	public static void validate(Select select) {
		new SelectValidator().doValidate(select);
	}

	private void doValidate(Select select) {

		select.visit(this);

		if (selectFieldCount == 0) {
			throw new IllegalStateException("SELECT does not declare a select list");
		}

		for (TableLike table : requiredBySelect) {
			if (!join.contains(table) && !from.contains(table)) {
				throw new IllegalStateException(String
						.format("Required table [%s] by a SELECT column not imported by FROM %s or JOIN %s", table, from, join));
			}
		}

		for (Table table : requiredByWhere) {
			if (!join.contains(table) && !from.contains(table)) {
				throw new IllegalStateException(String
						.format("Required table [%s] by a WHERE predicate not imported by FROM %s or JOIN %s", table, from, join));
			}
		}

		for (TableLike table : requiredByOrderBy) {
			if (!join.contains(table) && !from.contains(table)) {
				throw new IllegalStateException(String
						.format("Required table [%s] by a ORDER BY column not imported by FROM %s or JOIN %s", table, from, join));
			}
		}
	}

	@Override
	public void enter(Visitable segment) {

		if (segment instanceof Select) {
			selects.push((Select) segment);
		}

		if (selects.size() > 1) {
			return;
		}

		if (segment instanceof Expression && parent instanceof Select) {
			selectFieldCount++;
		}

		if (segment instanceof AsteriskFromTable && parent instanceof Select) {

			TableLike table = ((AsteriskFromTable) segment).getTable();
			requiredBySelect.add(table);
		}

		if (segment instanceof Column && (parent instanceof Select || parent instanceof SimpleFunction)) {

			TableLike table = ((Column) segment).getTable();

			if (table != null) {
				requiredBySelect.add(table);
			}
		}

		if (segment instanceof Column && parent instanceof OrderByField) {

			TableLike table = ((Column) segment).getTable();

			if (table != null) {
				requiredByOrderBy.add(table);
			}
		}

		if (segment instanceof TableLike && parent instanceof Join) {
			join.add((TableLike) segment);
		}
		super.enter(segment);
	}

	@Override
	public void leave(Visitable segment) {

		if (segment instanceof Select) {
			selects.remove(segment);
		}

		if (selects.size() > 1) {
			return;
		}

		super.leave(segment);
	}
}
