package org.springframework.data.relational.core.dialect.condition;

import org.springframework.data.relational.core.query.Criteria;

/**
 * This interface represents dialect specific conditions used in WHERE causes built by {@link Criteria}.
 *
 * @author Mikhail Polivakha
 */
public interface DialectCriteriaCondition {

    /**
     * Render a vendor-specific part of the SQL condition.
     *
     * @return the rendered part of the SQL statement
     */
    String render();
}
