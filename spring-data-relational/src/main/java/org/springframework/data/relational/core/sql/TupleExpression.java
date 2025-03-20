/*
 * Copyright 2025 the original author or authors.
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

import static java.util.stream.Collectors.*;

import java.util.List;

/**
 * A tuple as used in conditions like
 *
 * <pre>
 *   WHERE (one, two) IN (select x, y from some_table)
 * </pre>
 *
 * @author Jens Schauder
 * @since 3.5
 */
public class TupleExpression extends AbstractSegment implements Expression {

	private final List<? extends Expression> expressions;

	private static Segment[] children(List<? extends Expression> expressions) {
		return expressions.toArray(new Segment[0]);
	}

	TupleExpression(List<? extends Expression> expressions) {

		super(children(expressions));

		this.expressions = expressions;
	}

	public static TupleExpression create(Expression... expressions) {
		return new TupleExpression(List.of(expressions));
	}

	public static TupleExpression create(List<? extends Expression> expressions) {
		return new TupleExpression(expressions);
	}

	@Override
	public String toString() {
		return "(" + expressions.stream().map(Expression::toString).collect(joining(", ")) + ")";
	}
}
