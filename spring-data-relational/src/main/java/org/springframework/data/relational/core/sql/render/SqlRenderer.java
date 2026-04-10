/*
 * Copyright 2019-present the original author or authors.
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
import org.springframework.data.relational.core.sql.Upsert;
import org.springframework.util.Assert;

/**
 * SQL renderer for {@link Select} and {@link Delete} statements.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Christoph Strobl
 * @since 1.1
 * @see RenderContext
 */
public class SqlRenderer implements Renderer {

	private final RenderContext context;

	private SqlRenderer(RenderContext context) {

		Assert.notNull(context, "RenderContext must not be null");
		this.context = context;
	}

	/**
	 * Creates a new {@link SqlRenderer}.
	 *
	 * @return the renderer.
	 */
	public static SqlRenderer create() {
		return new SqlRenderer(new SimpleRenderContext(NamingStrategies.asIs()));
	}

	/**
	 * Creates a new {@link SqlRenderer} using a {@link RenderContext}.
	 *
	 * @param context must not be {@literal null}.
	 * @return the renderer.
	 */
	public static SqlRenderer create(RenderContext context) {
		return new SqlRenderer(context);
	}

	/**
	 * Renders a {@link Select} statement into its SQL representation.
	 *
	 * @param select must not be {@literal null}.
	 * @return the rendered statement.
	 */
	public static String toString(Select select) {
		return create().render(select);
	}

	/**
	 * Renders a {@link Insert} statement into its SQL representation.
	 *
	 * @param insert must not be {@literal null}.
	 * @return the rendered statement.
	 */
	public static String toString(Insert insert) {
		return create().render(insert);
	}

	/**
	 * Renders a {@link Update} statement into its SQL representation.
	 *
	 * @param update must not be {@literal null}.
	 * @return the rendered statement.
	 */
	public static String toString(Update update) {
		return create().render(update);
	}

	/**
	 * Renders a {@link Upsert} statement into its Standard SQL representation.
	 *
	 * @param update must not be {@literal null}.
	 * @return the rendered statement.
	 * @since 4.1
	 */
	public static String toString(Upsert update) {
		return create().render(update);
	}

	/**
	 * Renders a {@link Delete} statement into its SQL representation.
	 *
	 * @param delete must not be {@literal null}.
	 * @return the rendered statement.
	 */
	public static String toString(Delete delete) {
		return create().render(delete);
	}

	@Override
	public String render(Select select) {
		return new SelectStatementVisitor(context).render(select);
	}

	@Override
	public String render(Insert insert) {
		return new InsertStatementVisitor(context).render(insert);
	}

	@Override
	public String render(Update update) {
		return new UpdateStatementVisitor(context).render(update);
	}

	@Override
	public String render(Upsert upsert) {
		return new UpsertStatementVisitor(context).render(upsert);
	}

	@Override
	public String render(Delete delete) {
		return new DeleteStatementVisitor(context).render(delete);
	}

}
