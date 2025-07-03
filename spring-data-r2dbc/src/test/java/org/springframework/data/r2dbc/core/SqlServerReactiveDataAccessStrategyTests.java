/*
 * Copyright 2019-2025 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import org.springframework.data.r2dbc.dialect.SqlServerDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;

/**
 * {@link SqlServerDialect} specific tests for {@link ReactiveDataAccessStrategy}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class SqlServerReactiveDataAccessStrategyTests extends ReactiveDataAccessStrategyTestSupport {

	private final ReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(SqlServerDialect.INSTANCE);

	{
		((R2dbcMappingContext) strategy.getConverter().getMappingContext()).setForceQuote(false);
	}

	@Override
	protected ReactiveDataAccessStrategy getStrategy() {
		return strategy;
	}
}
