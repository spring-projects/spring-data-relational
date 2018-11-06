package org.springframework.data.jdbc.repository.support;

import org.springframework.data.jdbc.repository.query.Query;

/**
 * Exception thrown when both {@link Query#resultSetExtractorClass()} and {@link Query#rowMapperClass()} are used in one {@link Query}.
 *
 * @author Evgeni Dimitrov
 */
public class InvalidQueryConfiguration extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6604189906427682546L;

	public InvalidQueryConfiguration(String message) {
		super(message);
	}
}
