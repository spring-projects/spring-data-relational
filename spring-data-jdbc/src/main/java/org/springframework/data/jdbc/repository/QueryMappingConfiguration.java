package org.springframework.data.jdbc.repository;

import org.springframework.data.jdbc.support.RowMapperResultsetExtractorEither;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * A map from a type to a {@link ResultSetExtractor} to be used for extracting that type from {@link java.sql.ResultSet}s.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 */
public interface QueryMappingConfiguration {
	<T> RowMapperResultsetExtractorEither<?> getMapper(Class<T> type);
	
	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	QueryMappingConfiguration EMPTY = new QueryMappingConfiguration() {

		@Override
		public <T> RowMapperResultsetExtractorEither<?> getMapper(Class<T> type) {
			return null;
		}

	};

}
