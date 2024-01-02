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
package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Update;

/**
 * SQL renderer for {@link Select} and {@link Delete} statements.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public interface Renderer {

	/**
	 * Render the {@link Select} AST into a SQL statement.
	 *
	 * @param select the statement to render, must not be {@literal null}.
	 * @return the rendered statement.
	 */
	String render(Select select);

	/**
	 * Render the {@link Insert} AST into a SQL statement.
	 *
	 * @param insert the statement to render, must not be {@literal null}.
	 * @return the rendered statement.
	 */
	String render(Insert insert);

	/**
	 * Render the {@link Update} AST into a SQL statement.
	 *
	 * @param update the statement to render, must not be {@literal null}.
	 * @return the rendered statement.
	 */
	String render(Update update);

	/**
	 * Render the {@link Delete} AST into a SQL statement.
	 *
	 * @param delete the statement to render, must not be {@literal null}.
	 * @return the rendered statement.
	 */
	String render(Delete delete);
}
