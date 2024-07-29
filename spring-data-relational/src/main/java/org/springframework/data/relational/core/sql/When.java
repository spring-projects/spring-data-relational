package org.springframework.data.relational.core.sql;

/**
 * When segment for Case statement.
 * <p>
 * Results in a rendered condition: {@code WHEN <condition> THEN <value>}.
 * </p>
 */
public class When extends AbstractSegment {
    private final Expression condition;
    private final Literal value;

    private When(Expression condition, Literal value) {
        super(condition, value);
        this.condition = condition;
        this.value = value;
    }

    /**
     * Creates a new {@link When} given two {@link Expression} condition and {@link Literal} value.
     *
     * @param condition the condition {@link Expression}.
     * @param value     the {@link Literal} value.
     * @return the {@link When}.
     */
    public static When when(Expression condition, Literal value) {
        return new When(condition, value);
    }

    /**
     * @return the condition
     */
    public Expression getCondition() {
        return condition;
    }

    /**
     * @return the value
     */
    public Literal getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WHEN " + condition + " THEN " + value;
    }
}
