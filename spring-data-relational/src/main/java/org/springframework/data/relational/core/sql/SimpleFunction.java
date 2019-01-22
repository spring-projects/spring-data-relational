/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.util.StringUtils;

/**
 * Simple function accepting one or more {@link Expression}s.
 *
 * @author Mark Paluch
 */
public class SimpleFunction extends AbstractSegment implements Expression {

	private String functionName;
	private List<Expression> expressions;

	SimpleFunction(String functionName, List<Expression> expressions) {
		this.functionName = functionName;
		this.expressions = expressions;
	}

	/**
	 * Expose this function result under a column {@code alias}.
	 *
	 * @param alias column alias name, must not {@literal null} or empty.
	 * @return the aliased {@link SimpleFunction}.
	 */
	public SimpleFunction as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedFunction(functionName, expressions, alias);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null!");

		visitor.enter(this);
		expressions.forEach(it -> it.visit(visitor));
		visitor.leave(this);
	}

	/**
	 * @return the function name.
	 */
	public String getFunctionName() {
		return functionName;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return functionName + "(" + StringUtils.collectionToDelimitedString(expressions, ", ") + ")";
	}

	/**
	 * {@link Aliased} {@link SimpleFunction} implementation.
	 */
	static class AliasedFunction extends SimpleFunction implements Aliased {

		private final String alias;

		AliasedFunction(String functionName, List<Expression> expressions, String alias) {
			super(functionName, expressions);
			this.alias = alias;
		}

		@Override
		public String getAlias() {
			return alias;
		}
	}
}
