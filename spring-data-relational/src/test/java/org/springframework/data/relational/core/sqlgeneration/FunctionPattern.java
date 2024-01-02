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
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A pattern matching a function call.
 *
 * @author Jens Schauder
 */
public final class FunctionPattern extends TypedExpressionPattern<Function> {
	private final String name;
	private final List<ExpressionPattern> params;

	/**
	 * @param name   name of the function.
	 * @param params patterns to match the function arguments.
	 */
	public FunctionPattern(String name, List<ExpressionPattern> params) {

		super(Function.class);

		this.name = name;
		this.params = params;
	}

	FunctionPattern(String name, ExpressionPattern... params) {
		this(name, Arrays.asList(params));
	}


	@Override
	public boolean matches(Function function) {

			if (function.getName().equalsIgnoreCase(name)) {
				List<Expression> expressions = new ArrayList<>(function.getParameters().getExpressions());
				for (ExpressionPattern param : params) {
					boolean found = false;
					for (Expression exp : expressions) {
						if (param.matches(exp)) {
							expressions.remove(exp);
							found = true;
							break;
						}
					}
					if (!found) {
						return false;
					}
				}

				return expressions.isEmpty();
			}
			return false;
	}

	@Override
	public String toString() {
		return name + "(" + params.stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
	}

	public String name() {
		return name;
	}

	public List<ExpressionPattern> params() {
		return params;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) return true;
		if (obj == null || obj.getClass() != this.getClass()) return false;
		var that = (FunctionPattern) obj;
		return Objects.equals(this.name, that.name) &&
				Objects.equals(this.params, that.params);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, params);
	}

}
