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

import org.springframework.data.relational.core.sql.Visitor;

/**
 * Callback interface for {@link Visitor visitors} that wish to notify a render target when they are complete with
 * rendering.
 *
 * @author Mark Paluch
 * @since 1.1
 */
@FunctionalInterface
interface RenderTarget {

	/**
	 * Callback method that is invoked once the rendering for a part or expression is finished. When called multiple
	 * times, it's the responsibility of the implementor to ensure proper concatenation of render results.
	 *
	 * @param sequence the rendered part or expression.
	 */
	void onRendered(CharSequence sequence);
}
