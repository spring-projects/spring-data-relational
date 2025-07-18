package org.springframework.data.relational.core.mapping;

import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.util.Assert;

/**
 * Provide support for processing SpEL expressions in @Table and @Column annotations, or anywhere we want to use SpEL
 * expressions and sanitize the result of the evaluated SpEL expression. The default sanitization allows for digits,
 * alphabetic characters and _ characters and strips out any other characters. Custom sanitization (if desired) can be
 * achieved by creating a class that implements the {@link SqlIdentifierSanitizer} interface and then invoking the
 * {@link #setSanitizer(SqlIdentifierSanitizer)} method.
 *
 * @author Kurt Niemi
 * @author Sergey Korotaev
 * @author Mark Paluch
 * @see SqlIdentifierSanitizer
 * @since 3.2
 */
class SqlIdentifierExpressionEvaluator {

	private EvaluationContextProvider provider;

	private SqlIdentifierSanitizer sanitizer = SqlIdentifierSanitizer.words();
	private Environment environment = new StandardEnvironment();

	public SqlIdentifierExpressionEvaluator(EvaluationContextProvider provider) {
		this.provider = provider;
	}

	public SqlIdentifier evaluate(ValueExpression expression, boolean isForceQuote) throws EvaluationException {

		Assert.notNull(expression, "Expression must not be null.");

		EvaluationContext evaluationContext = provider.getEvaluationContext(null);
		ValueEvaluationContext valueEvaluationContext = ValueEvaluationContext.of(environment, evaluationContext);

		Object value = expression.evaluate(valueEvaluationContext);
		if (value instanceof SqlIdentifier sqlIdentifier) {
			return sqlIdentifier;
		}

		if (value == null) {
			throw new EvaluationException("Expression '%s' evaluated to 'null'".formatted(expression));
		}

		String sanitizedResult = sanitizer.sanitize(value.toString());
		return isForceQuote ? SqlIdentifier.quoted(sanitizedResult) : SqlIdentifier.unquoted(sanitizedResult);
	}

	public void setSanitizer(SqlIdentifierSanitizer sanitizer) {

		Assert.notNull(sanitizer, "SqlIdentifierSanitizer must not be null");

		this.sanitizer = sanitizer;
	}

	public void setProvider(EvaluationContextProvider provider) {

		Assert.notNull(provider, "EvaluationContextProvider must not be null");

		this.provider = provider;
	}

	public void setEnvironment(Environment environment) {

		Assert.notNull(environment, "Environment must not be null");

		this.environment = environment;
	}
}
