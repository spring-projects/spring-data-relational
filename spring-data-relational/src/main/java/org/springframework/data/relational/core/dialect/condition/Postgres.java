package org.springframework.data.relational.core.dialect.condition;

import org.springframework.data.relational.core.dialect.PostgresDialect;

/**
 * {@link DialectCriteriaCondition DialectCriteriaConditions} that are specific to {@link PostgresDialect PostgreSQL Dialect}
 *
 * @author Mikhail Polivakha
 */
public class Postgres {

    /**
     * Creates a condition that checks that the assumed column of an {@link java.sql.Array} type
     * contains an array of any values
     *
     * @param values array that assumed column should contain
     * @return crafted {@link DialectCriteriaCondition}
     */
    public static DialectCriteriaCondition arrayContains(Object... values) {
        return () -> "@> ARRAY[%s]".formatted(toLiterals(false, values));
    }

    /**
     * Creates a condition that checks that the assumed column of an {@link java.sql.Array} type
     * contains an array of {@link String} values.
     *
     * @param values array of {@link String String} that assumed column should contain
     * @return crafted {@link DialectCriteriaCondition}
     */
    public static DialectCriteriaCondition arrayContains(String... values) {
        return () -> "@> ARRAY[%s]::text[]".formatted(toLiterals(true, values));
    }

    /**
     * Creates a condition that checks that the assumed column of an {@link java.sql.Array} type
     * contains an array of a single {@link String} value.
     *
     * @param value array of {@link String String} that assumed value should contain
     * @return crafted {@link DialectCriteriaCondition}
     */
    public static DialectCriteriaCondition arrayContains(String value) {
        return arrayContains(new String[]{value});
    }

    @SafeVarargs
    private static  <T> String toLiterals(boolean quoted, T... values) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            T value = values[i];

            if (value != null) {
                if (quoted) {
                    result.append('\'').append(value).append('\'');
                } else {
                    result.append(value);
                }
            } else {
                result.append("NULL");
            }

            if (i != values.length - 1) {
                result.append(",");
            }
        }
        return result.toString();
    }
}
