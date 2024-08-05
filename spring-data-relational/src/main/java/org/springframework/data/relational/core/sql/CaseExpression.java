package org.springframework.data.relational.core.sql;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Case with one or more conditions expression.
 * <p>
 * Results in a rendered condition:
 * <pre>
 *   CASE
 *     WHEN condition1 THEN result1
 *     WHEN condition2 THEN result2
 *     ELSE result
 *   END
 * </pre>
 * </p>
 *
 * @author Sven Rienstra
 * @since 3.4
 */
public class CaseExpression extends AbstractSegment implements Expression {
    private final List<When> whenList;
    private final Expression elseExpression;

    private CaseExpression(List<When> whenList, Expression elseExpression) {

        super(children(whenList, elseExpression));
        this.whenList = whenList;
        this.elseExpression = elseExpression;
    }

    /**
     * Create CASE {@link Expression} with initial {@link When} condition.
     * @param condition initial {@link When} condition
     * @return the {@link CaseExpression}
     */
    public static CaseExpression create(When condition) {
        return new CaseExpression(List.of(condition), null);
    }

    /**
     * Add additional {@link When} condition
     * @param condition the {@link When} condition
     * @return the {@link CaseExpression}
     */
    public CaseExpression when(When condition) {
        List<When> conditions = new ArrayList<>(this.whenList);
        conditions.add(condition);
        return new CaseExpression(conditions, elseExpression);
    }

    /**
     * Add ELSE clause
     * @param elseExpression the {@link Expression} else value
     * @return the {@link CaseExpression}
     */
    public CaseExpression elseExpression(Literal elseExpression) {
        return new CaseExpression(whenList, elseExpression);
    }

    /**
     * @return the {@link When} conditions
     */
    public List<When> getWhenList() {
        return whenList;
    }

    /**
     * @return the ELSE {@link Literal} value
     */
    public Expression getElseExpression() {
        return elseExpression;
    }

    @Override
    public String toString() {
        return "CASE " + whenList.stream().map(When::toString).collect(joining(" ")) + (elseExpression != null ? " ELSE " + elseExpression : "") + " END";
    }

    private static Segment[] children(List<When> whenList, Expression elseExpression) {

        List<Segment> segments = new ArrayList<>();
        segments.addAll(whenList);

        if (elseExpression != null) {
            segments.add(elseExpression);
        }

        return segments.toArray(new Segment[segments.size()]);
    }
}
