package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * Specify the sequence from which the value for the {@link org.springframework.data.annotation.Id}
 * should be fetched
 *
 * @author Mikhail Polivakha
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface TargetSequence {

    /**
     * The name of the sequence from which the id should be fetched
     */
    String value() default "";

    /**
     * Alias for {@link #value()}
     */
    @AliasFor("value")
    String sequence() default "";

    /**
     * Schema where the sequence reside.
     * Technically, this attribute is not necessarily the schema. It just represents the location/namespace,
     * where the sequence resides. For instance, in Oracle databases the schema and user are often used
     * interchangeably, so {@link #schema() schema} attribute may represent an Oracle user as well.
     * <p>
     * The final name of the sequence to be queried for the next value will be constructed by the concatenation
     * of schema and sequence : <pre>schema().sequence()</pre>
     */
    String schema() default "";
}
