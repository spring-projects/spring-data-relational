package org.springframework.data.relational.core.mapping;

import java.util.regex.Pattern;

import org.springframework.util.Assert;

/**
 * Functional interface to sanitize SQL identifiers for SQL usage. Useful to guard SpEL expression results.
 *
 * @author Kurt Niemi
 * @author Mark Paluch
 * @since 3.2
 * @see RelationalMappingContext#setSqlIdentifierSanitizer(SqlIdentifierSanitizer)
 */
@FunctionalInterface
public interface SqlIdentifierSanitizer {

	/**
	 * A sanitizer to allow words only. Non-words are removed silently.
	 *
	 * @return
	 */
	static SqlIdentifierSanitizer words() {

		Pattern pattern = Pattern.compile("[^\\w_]");

		return name -> {

			Assert.notNull(name, "Input to sanitize must not be null");

			return pattern.matcher(name).replaceAll("");
		};
	}

	/**
	 * Sanitize a SQL identifier to either remove unwanted character sequences or to throw an exception.
	 *
	 * @param sqlIdentifier the identifier name.
	 * @return sanitized SQL identifier.
	 */
	String sanitize(String sqlIdentifier);
}
