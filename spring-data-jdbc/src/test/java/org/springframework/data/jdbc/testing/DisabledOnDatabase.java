package org.springframework.data.jdbc.testing;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.EnabledIf;

/**
 * Annotation that allows to disable a particular test to be executed on a particular database
 *
 * @author Mikhail Polivakha
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(DisabledOnDatabaseExecutionCondition.class)
public @interface DisabledOnDatabase {

    /**
     * The database on which the test is not supposed to run on
     */
    DatabaseType database();
}
