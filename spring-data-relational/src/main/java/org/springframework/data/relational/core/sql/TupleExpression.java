package org.springframework.data.relational.core.sql;

import org.jetbrains.annotations.NotNull;

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

	private TupleExpression(List<? extends Expression> expressions) {

		super(children(expressions));

		this.expressions = expressions;
	}

	public static TupleExpression create(Expression... expressions) {
		return new TupleExpression(List.of(expressions));
	}

	public static TupleExpression create(List<? extends Expression> expressions) {
		return new TupleExpression(expressions);
	}

	public static Expression maybeWrap(List<Column> columns) {

		if (columns.size() == 1) {
			return columns.get(0);
		}
		return new TupleExpression(columns);
	}

	@Override
	public String toString() {
		return "(" + expressions.stream().map(Expression::toString).collect(joining(", ")) + ")";
	}
}
