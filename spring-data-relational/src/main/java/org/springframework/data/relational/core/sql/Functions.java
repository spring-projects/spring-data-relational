/*
 * Copyright 2019-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.util.Assert;

/**
 * Factory for common {@link Expression function expressions}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see SQL
 * @see Expressions
 * @see Functions
 */
public class Functions {

	/**
	 * Creates a new {@code COUNT} function.
	 *
	 * @param columns columns to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction count function} for {@code columns}.
	 */
	public static SimpleFunction count(Expression... columns) {

		Assert.notNull(columns, "Columns must not be null");
		Assert.notEmpty(columns, "Columns must contains at least one column");

		return SimpleFunction.create("COUNT", Arrays.asList(columns));
	}

	public static SimpleFunction least(Expression... expressions) {

		return SimpleFunction.create("LEAST", Arrays.asList(expressions));
	}

	/**
	 * Creates a new {@code COUNT} function.
	 *
	 * @param columns columns to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction count function} for {@code columns}.
	 */
	public static SimpleFunction count(Collection<? extends Expression> columns) {

		Assert.notNull(columns, "Columns must not be null");

		return SimpleFunction.create("COUNT", new ArrayList<>(columns));
	}

	/**
	 * Creates a new {@code UPPER} function.
	 *
	 * @param expression expression to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction upper function} for {@code expression}.
	 * @since 2.0
	 */
	public static SimpleFunction upper(Expression expression) {

		Assert.notNull(expression, "Expression must not be null");

		return SimpleFunction.create("UPPER", Collections.singletonList(expression));
	}

	/**
	 * Creates a new {@code LOWER} function.
	 *
	 * @param expression expression to apply lower, must not be {@literal null}.
	 * @return the new {@link SimpleFunction lower function} for {@code expression}.
	 * @since 2.0
	 */
	public static SimpleFunction lower(Expression expression) {

		Assert.notNull(expression, "Columns must not be null");

		return SimpleFunction.create("LOWER", Collections.singletonList(expression));
	}

	// Utility constructor.
	private Functions() {}
}
