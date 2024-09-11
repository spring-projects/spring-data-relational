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

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;

/**
 * Pattern matching analytic functions
 * 
 * @author Jens Schauder
 */
public class AnalyticFunctionPattern extends TypedExpressionPattern<AnalyticExpression> {

	private final ExpressionPattern partitionBy;
	private String functionName;

	public AnalyticFunctionPattern(String rowNumber, ExpressionPattern partitionBy) {

		super(AnalyticExpression.class);

		this.functionName = rowNumber;
		this.partitionBy = partitionBy;
	}

	@Override
	public boolean matches(SelectItem selectItem) {

		Expression expression = selectItem.getExpression();
		if (expression instanceof AnalyticExpression analyticExpression) {
			return matches(analyticExpression);
		}

		return false;
	}

	@Override
	boolean matches(AnalyticExpression analyticExpression) {
		return analyticExpression.getName().toLowerCase().equals(functionName) && partitionByMatches(analyticExpression);
	}

	private boolean partitionByMatches(AnalyticExpression analyticExpression) {
		
		List<? extends Expression> expressions = analyticExpression.getPartitionExpressionList();
		return expressions != null && expressions.size() == 1 && partitionBy.matches(expressions.get(0));
	}

	@Override
	public String toString() {
		return "row_number() OVER (PARTITION BY " + partitionBy + ')';
	}
}
