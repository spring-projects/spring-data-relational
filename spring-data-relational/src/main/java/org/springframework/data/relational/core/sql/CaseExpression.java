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
 */
public class CaseExpression extends AbstractSegment implements Expression {
    private final List<When> whenList = new ArrayList<>();
    private final Literal other;

    private CaseExpression(List<When> whenList, Literal other) {
        super(children(whenList, other));
        this.whenList.addAll(whenList);
        this.other = other;
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
        this.whenList.add(condition);
        return this;
    }

    /**
     * Add ELSE clause
     * @param other the {@link Literal} else value
     * @return the {@link CaseExpression}
     */
    public CaseExpression other(Literal other) {
        return new CaseExpression(whenList, other);
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
    public Literal getOther() {
        return other;
    }

    @Override
    public String toString() {
        return "CASE " + whenList.stream().map(When::toString).collect(joining(" ")) + (other != null ? " ELSE " + other : "") + " END";
    }

    private static Segment[] children(List<When> whenList, Literal other) {
        List<Segment> segments = new ArrayList<>();
        segments.addAll(whenList);
        segments.add(other);
        return segments.toArray(new Segment[segments.size()]);
    }
}
