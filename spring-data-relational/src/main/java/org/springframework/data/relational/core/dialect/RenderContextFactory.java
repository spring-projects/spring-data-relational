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

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.render.NamingStrategies;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.RenderNamingStrategy;
import org.springframework.data.relational.core.sql.render.SelectRenderContext;
import org.springframework.util.Assert;

/**
 * Factory for {@link RenderContext} based on {@link Dialect}.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @author Jens Schauder
 * @since 1.1
 */
public class RenderContextFactory {

	public final Dialect dialect;

	public RenderNamingStrategy namingStrategy = NamingStrategies.asIs();

	/**
	 * Creates a new {@link RenderContextFactory} given {@link Dialect}.
	 *
	 * @param dialect must not be {@literal null}.
	 */
	public RenderContextFactory(Dialect dialect) {

		Assert.notNull(dialect, "Dialect must not be null");

		this.dialect = dialect;
	}

	/**
	 * Set a {@link RenderNamingStrategy}.
	 *
	 * @param namingStrategy must not be {@literal null}.
	 */
	public void setNamingStrategy(RenderNamingStrategy namingStrategy) {

		Assert.notNull(namingStrategy, "RenderNamingStrategy must not be null");

		this.namingStrategy = namingStrategy;
	}

	/**
	 * Returns a {@link RenderContext} configured with {@link Dialect} specifics.
	 *
	 * @return the {@link RenderContext}.
	 */
	public RenderContext createRenderContext() {

		SelectRenderContext selectRenderContext = dialect.getSelectContext();

		return new DialectRenderContext(namingStrategy, dialect, selectRenderContext);
	}

	/**
	 * {@link RenderContext} derived from {@link Dialect} specifics.
	 */
	static class DialectRenderContext implements RenderContext {

		private final RenderNamingStrategy renderNamingStrategy;
		private final Dialect renderingDialect;
		private final SelectRenderContext selectRenderContext;
		private final InsertRenderContext insertRenderContext;

		DialectRenderContext(RenderNamingStrategy renderNamingStrategy, Dialect renderingDialect,
				SelectRenderContext selectRenderContext) {

			Assert.notNull(renderNamingStrategy, "RenderNamingStrategy must not be null");
			Assert.notNull(renderingDialect, "renderingDialect must not be null");
			Assert.notNull(renderingDialect.getIdentifierProcessing(),
					"IdentifierProcessing of renderingDialect must not be null");
			Assert.notNull(selectRenderContext, "SelectRenderContext must not be null");

			this.renderNamingStrategy = renderNamingStrategy;
			this.renderingDialect = renderingDialect;
			this.selectRenderContext = selectRenderContext;
			this.insertRenderContext = renderingDialect.getInsertRenderContext();
		}

		@Override
		public RenderNamingStrategy getNamingStrategy() {
			return renderNamingStrategy;
		}

		@Override
		public IdentifierProcessing getIdentifierProcessing() {
			return renderingDialect.getIdentifierProcessing();
		}

		@Override
		public SelectRenderContext getSelectRenderContext() {
			return selectRenderContext;
		}

		@Override
		public InsertRenderContext getInsertRenderContext() {
			return insertRenderContext;
		}
	}
}
