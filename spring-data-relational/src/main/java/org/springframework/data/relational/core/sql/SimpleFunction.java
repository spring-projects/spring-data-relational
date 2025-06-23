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
package org.springframework.data.relational.core.sql;

import java.util.List;

import org.springframework.util.Assert;

/**
 * Simple function accepting one or more {@link Expression}s.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class SimpleFunction extends BaseFunction implements Expression {

	private SimpleFunction(String functionName, List<? extends Expression> expressions) {
		super(functionName, "(", ")", expressions);
	}

	/**
	 * Creates a new {@link SimpleFunction} given {@code functionName} and {@link List} of {@link Expression}s.
	 *
	 * @param functionName must not be {@literal null}.
	 * @param expressions zero or many {@link Expression}s, must not be {@literal null}.
	 * @return
	 */
	public static SimpleFunction create(String functionName, List<? extends Expression> expressions) {

		Assert.hasText(functionName, "Function name must not be null or empty");
		Assert.notNull(expressions, "Expressions name must not be null");

		return new SimpleFunction(functionName, expressions);
	}

}
