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
package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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

		Assert.notNull(columns, "Columns must not be null!");
		Assert.notEmpty(columns, "Columns must contains at least one column");

		return SimpleFunction.create("COUNT", Arrays.asList(columns));
	}

	/**
	 * Creates a new {@code COUNT} function.
	 *
	 * @param columns columns to apply count, must not be {@literal null}.
	 * @return the new {@link SimpleFunction count function} for {@code columns}.
	 */
	public static SimpleFunction count(Collection<? extends Expression> columns) {

		Assert.notNull(columns, "Columns must not be null!");

		return SimpleFunction.create("COUNT", new ArrayList<>(columns));
	}

	// Utility constructor.
	private Functions() {}
}
