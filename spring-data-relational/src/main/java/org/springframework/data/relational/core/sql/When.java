package org.springframework.data.relational.core.sql;

/**
 * When segment for Case statement.
 * <p>
 * Results in a rendered condition: {@code WHEN <condition> THEN <value>}.
 * </p>
 *
 * @author Sven Rienstra
 * @since 3.4
 */
public class When extends AbstractSegment {

    private final Condition condition;
    private final Expression value;

    private When(Condition condition, Expression value) {

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
    public static When when(Condition condition, Expression value) {
        return new When(condition, value);
    }

    /**
     * @return the condition
     */
    public Condition getCondition() {
        return condition;
    }

    /**
     * @return the value
     */
    public Expression getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "WHEN " + condition + " THEN " + value;
    }
}
