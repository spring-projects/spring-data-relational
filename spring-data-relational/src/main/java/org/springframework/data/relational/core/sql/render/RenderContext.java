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

import org.springframework.data.relational.core.dialect.InsertRenderContext;
import org.springframework.data.relational.core.sql.IdentifierProcessing;

/**
 * Render context providing {@link RenderNamingStrategy} and other resources that are required during rendering.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @author Jens Schauder
 * @since 1.1
 */
public interface RenderContext {

	/**
	 * Returns the configured {@link RenderNamingStrategy}.
	 *
	 * @return the {@link RenderNamingStrategy}.
	 */
	RenderNamingStrategy getNamingStrategy();

	/**
	 * Returns the configured {@link IdentifierProcessing}.
	 *
	 * @return the {@link IdentifierProcessing}.
	 * @since 2.0
	 */
	IdentifierProcessing getIdentifierProcessing();

	/**
	 * @return the {@link SelectRenderContext}.
	 */
	SelectRenderContext getSelectRenderContext();

	/**
	 * @return the {@link InsertRenderContext}
	 */
	InsertRenderContext getInsertRenderContext();
}
