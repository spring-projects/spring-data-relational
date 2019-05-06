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
package org.springframework.data.r2dbc.function;

import lombok.RequiredArgsConstructor;

import org.springframework.data.r2dbc.dialect.Dialect;
import org.springframework.data.relational.core.sql.render.RenderContext;

/**
 * Default {@link StatementFactory} implementation.
 *
 * @author Mark Paluch
 */
// TODO: Move DefaultPreparedOperation et al to a better place. Probably StatementMapper.
@RequiredArgsConstructor
class DefaultStatementFactory {

	private final Dialect dialect;
	private final RenderContext renderContext;

}
