package org.springframework.data.relational.core.mapping;

/**
 * Interface for sanitizing Spel Expression results
 *
 * @author Kurt Niemi
 */
public interface SpelExpressionResultSanitizer {
    public String sanitize(String result);
}
