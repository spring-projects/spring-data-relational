/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;

import java.util.Collection;

import org.assertj.core.api.AbstractAssert;

/**
 * AspectJ {@link org.assertj.core.api.Assert} for writing assertions about joins in SQL statements.
 * 
 * @author Jens Schauder
 */
public class JoinAssert extends AbstractAssert<JoinAssert, Join> {
	public JoinAssert(Join join) {
		super(join, JoinAssert.class);
	}

	JoinAssert on(String left, String right) {

		Collection<Expression> onExpressions = actual.getOnExpressions();

		if (!(onExpressions.iterator().next().toString().equals(left + " = " + right))) {
			throw failureWithActualExpected(actual, left + " = " + right,
					"actual join condition %s does not match expected %s = %s", actual, left, right);
		}
		return this;
	}
}
