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

/**
 * Validator for {@link Delete} statements.
 * <p>
 * Validates that all {@link Column}s using a table qualifier have a table import from the {@code FROM} clause.
 * </p>
 * 
 * @author Mark Paluch
 * @since 1.1
 */
class DeleteValidator extends AbstractImportValidator {

	/**
	 * Validates a {@link Delete} statement.
	 *
	 * @param delete the {@link Delete} statement.
	 * @throws IllegalStateException if the statement is not valid.
	 */
	public static void validate(Delete delete) {
		new DeleteValidator().doValidate(delete);
	}

	private void doValidate(Delete select) {

		select.visit(this);

		for (Table table : requiredByWhere) {
			if (!from.contains(table)) {
				throw new IllegalStateException(
						String.format("Required table [%s] by a WHERE predicate not imported by FROM %s", table, from));
			}
		}
	}
}
