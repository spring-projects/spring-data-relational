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
package org.springframework.data.r2dbc.query;

import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.util.Assert;

/**
 * Value object representing a {@link Condition} with its {@link Bindings}.
 *
 * @author Mark Paluch
 */
public class BoundCondition {

	private final Bindings bindings;

	private final Condition condition;

	public BoundCondition(Bindings bindings, Condition condition) {

		Assert.notNull(bindings, "Bindings must not be null");
		Assert.notNull(condition, "Condition must not be null");

		this.bindings = bindings;
		this.condition = condition;
	}

	public Bindings getBindings() {
		return bindings;
	}

	public Condition getCondition() {
		return condition;
	}
}
