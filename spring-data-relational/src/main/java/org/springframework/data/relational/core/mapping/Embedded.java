package org.springframework.data.relational.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The annotation to configure a value object as embedded in the current table.
 *
 * @author Bastian Wilhelm
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Documented
public @interface Embedded {
  /**
   * @return prefix for columns in the embedded value object. Default is an empty String
   */
  String value() default "";
}
