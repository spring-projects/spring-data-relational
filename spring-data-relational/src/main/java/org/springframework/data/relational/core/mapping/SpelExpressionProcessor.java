package org.springframework.data.relational.core.mapping;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * Provide support for processing SpEL expressions in @Table and @Column annotations,
 * or anywhere we want to use SpEL expressions and sanitize the result of the evaluated
 * SpEL expression.
 *
 * The default sanitization allows for digits, alphabetic characters and _ characters
 * and strips out any other characters.
 *
 * Custom sanitization (if desired) can be achieved by creating a class that implements
 * the {@link #SpelExpressionResultSanitizer} interface and configuring a spelExpressionResultSanitizer
 * Bean.
 *
 * @author Kurt Niemi
 * @since 3.1
 */
public class SpelExpressionProcessor {
    @Autowired(required = false)
    private SpelExpressionResultSanitizer spelExpressionResultSanitizer;
    private StandardEvaluationContext evalContext = new StandardEvaluationContext();
    private SpelExpressionParser parser = new SpelExpressionParser();
    private TemplateParserContext templateParserContext = new TemplateParserContext();

    public String applySpelExpression(String expression) throws EvaluationException {

        Assert.notNull(expression, "Expression must not be null.");

        // Only apply logic if we have the prefixes/suffixes required for a SpEL expression as firstly
        // there is nothing to evaluate (i.e. whatever literal passed in is returned as-is) and more
        // importantly we do not want to perform any sanitization logic.
        if (!isSpellExpression(expression)) {
            return expression;
        }

        Expression expr = parser.parseExpression(expression, templateParserContext);
        String result = expr.getValue(evalContext, String.class);

        // Normally an exception is thrown by the Spel parser on invalid syntax/errors but this will provide
        // a consistent experience for any issues with Spel parsing.
        if (result == null) {
            throw new EvaluationException("Spel Parsing of expression \"" + expression + "\" failed.");
        }

        String sanitizedResult = getSpelExpressionResultSanitizer().sanitize(result);

        return sanitizedResult;
    }

    protected boolean isSpellExpression(String expression) {

        String trimmedExpression = expression.trim();
        if (trimmedExpression.startsWith(templateParserContext.getExpressionPrefix()) &&
                trimmedExpression.endsWith(templateParserContext.getExpressionSuffix())) {
            return true;
        }

        return false;
    }
    private SpelExpressionResultSanitizer getSpelExpressionResultSanitizer() {

        if (this.spelExpressionResultSanitizer == null) {
            this.spelExpressionResultSanitizer = new SpelExpressionResultSanitizer() {
                @Override
                public String sanitize(String result) {

                    String cleansedResult = result.replaceAll("[^\\w]", "");
                    return cleansedResult;
                }
            };
        }
        return this.spelExpressionResultSanitizer;
    }
}
