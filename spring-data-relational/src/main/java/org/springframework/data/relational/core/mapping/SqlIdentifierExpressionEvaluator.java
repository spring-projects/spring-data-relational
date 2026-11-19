package org.springframework.data.relational.core.mapping;

import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.expression.EvaluationException;
import org.springframework.util.Assert;

/**
 * Provide support for processing SpEL expressions in @Table and @Column annotations, or anywhere we want to use SpEL
 * expressions and sanitize the result of the evaluated SpEL expression. The default sanitization allows for letters,
 * combining marks, decimal digits and _ characters and strips out any other characters. Custom sanitization (if
 * desired) can be achieved by creating a class that implements the {@link SqlIdentifierSanitizer} interface and then
 * invoking the {@link #setSanitizer(SqlIdentifierSanitizer)} method.
 *
 * @author Kurt Niemi
 * @author Sergey Korotaev
 * @author Mark Paluch
 * @see SqlIdentifierSanitizer
 * @since 3.2
 */
class SqlIdentifierExpressionEvaluator {

	private final ValueEvaluationContextProvider provider;

	private SqlIdentifierSanitizer sanitizer = SqlIdentifierSanitizer.words();

	public SqlIdentifierExpressionEvaluator(ValueEvaluationContextProvider provider) {
		this.provider = provider;
	}

	public SqlIdentifier evaluate(ValueExpression expression, boolean isForceQuote) throws EvaluationException {

		Assert.notNull(expression, "Expression must not be null.");

		ValueEvaluationContext evaluationContext = provider.getEvaluationContext(null);
		Object value = expression.evaluate(evaluationContext);

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

}
