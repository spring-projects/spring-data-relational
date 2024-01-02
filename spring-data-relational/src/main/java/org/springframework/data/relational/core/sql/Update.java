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
 * AST for aa {@code UPDATE} statement. Visiting order:
 * <ol>
 * <li>Self</li>
 * <li>{@link Table table}</li>
 * <li>{@link Assignments assignments}</li>
 * <li>{@link Where WHERE} condition</li>
 * </ol>
 *
 * @author Mark Paluch
 * @since 1.1
 * @see StatementBuilder
 * @see SelectBuilder
 * @see SQL
 */
public interface Update extends Segment, Visitable {

	/**
	 * Creates a new {@link UpdateBuilder}.
	 *
	 * @return a new {@link UpdateBuilder}.
	 */
	static UpdateBuilder builder() {
		return new DefaultUpdateBuilder();
	}
}
