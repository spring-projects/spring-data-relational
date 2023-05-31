package org.springframework.data.relational.core.mapping;

import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.util.Assert;

/**
 * Provide support for processing SpEL expressions in @Table and @Column annotations, or anywhere we want to use SpEL
 * expressions and sanitize the result of the evaluated SpEL expression. The default sanitization allows for digits,
 * alphabetic characters and _ characters and strips out any other characters. Custom sanitization (if desired) can be
 * achieved by creating a class that implements the {@link SqlIdentifierSanitizer} interface and then invoking the
 * {@link #setSanitizer(SqlIdentifierSanitizer)} method.
 *
 * @author Kurt Niemi
 * @see SqlIdentifierSanitizer
 * @since 3.2
 */
class ExpressionEvaluator {

	private EvaluationContextProvider provider;

	private SqlIdentifierSanitizer sanitizer = SqlIdentifierSanitizer.words();

	public ExpressionEvaluator(EvaluationContextProvider provider) {
		this.provider = provider;
	}

	public String evaluate(Expression expression) throws EvaluationException {

		Assert.notNull(expression, "Expression must not be null.");

		String result = expression.getValue(provider.getEvaluationContext(null), String.class);
		return sanitizer.sanitize(result);
	}

	public void setSanitizer(SqlIdentifierSanitizer sanitizer) {

		Assert.notNull(sanitizer, "SqlIdentifierSanitizer must not be null");

		this.sanitizer = sanitizer;
	}

	public void setProvider(EvaluationContextProvider provider) {
		this.provider = provider;
	}
}
