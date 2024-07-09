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
package org.springframework.data.relational.core.sql;

/**
 * Wrapper for a {@link Select} query to be used as subselect.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public class SubselectExpression extends Subselect implements Expression {

	SubselectExpression(Select subselect) {

		super(subselect);
	}

	/**
	 * Wraps a Select in a {@link SubselectExpression}, for using it as an expression in function calls or similar.
	 *
	 * @author Jens Schauder
	 * @since 3.4
	 */
	public static Expression of(Select subselect) {
		return new SubselectExpression(subselect);
	}
}
