package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * Specify the sequence from which the value for e.g. an {@link org.springframework.data.annotation.Id} should be fetched.
 *
 * @author Mikhail Polivakha
 * @since 3.5
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Documented
public @interface Sequence {

	/**
	 * The name of the sequence from which the value should be fetched.
	 */
	@AliasFor("sequence")
	String value() default "";

	/**
	 * Alias for {@link #value()}
	 */
	@AliasFor("value")
	String sequence() default "";

	/**
	 * Schema where the sequence resides. For instance, in Oracle databases the schema and user are often used
	 * interchangeably, so the {@code schema} attribute may represent an Oracle user as well.
	 * <p>
	 * The final name of the sequence to be queried for the next value will be constructed by the concatenation of schema
	 * and sequence:
	 *
	 * <pre class="code">
	 * schema().sequence()
	 * </pre>
	 */
	String schema() default "";
}
