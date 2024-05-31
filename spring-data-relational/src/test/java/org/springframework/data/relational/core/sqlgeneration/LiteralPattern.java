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
import net.sf.jsqlparser.statement.select.SelectItem;

/**
 * Pattern matching a literal expression in a SQL statement.
 * 
 * @param value the value of the expression
 * @author Jens Schauder
 */
record LiteralPattern(Object value) implements SelectItemPattern, ExpressionPattern {

	@Override
	public boolean matches(SelectItem selectItem) {
		return matches(selectItem.getExpression());
	}

	@Override
	public boolean matches(Expression expression) {
		return expression.toString().equals(String.valueOf(value));
	}
}
