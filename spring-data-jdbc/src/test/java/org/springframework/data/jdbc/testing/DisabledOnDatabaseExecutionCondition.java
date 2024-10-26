package org.springframework.data.jdbc.testing;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * {@link ExecutionCondition} for the {@link DisabledOnDatabase} annotation
 *
 * @author Mikhail Polivakha
 */
public class DisabledOnDatabaseExecutionCondition implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        ApplicationContext applicationContext = SpringExtension.getApplicationContext(context);

        MergedAnnotation<DisabledOnDatabase> disabledOnDatabaseMergedAnnotation = MergedAnnotations
          .from(context.getRequiredTestMethod(), MergedAnnotations.SearchStrategy.DIRECT)
          .get(DisabledOnDatabase.class);

        DatabaseType database = disabledOnDatabaseMergedAnnotation.getEnum("database", DatabaseType.class);

        if (ArrayUtils.contains(applicationContext.getEnvironment().getActiveProfiles(), database.getProfile())) {
            return ConditionEvaluationResult.disabled(
                "The test method '%s' is disabled for '%s' because of the @DisabledOnDatabase annotation".formatted(context.getRequiredTestMethod().getName(), database)
            );
        }
        return ConditionEvaluationResult.enabled("The test method '%s' is enabled".formatted(context.getRequiredTestMethod()));
    }
}
