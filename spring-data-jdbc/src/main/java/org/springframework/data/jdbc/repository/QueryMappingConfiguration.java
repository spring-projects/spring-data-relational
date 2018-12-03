package org.springframework.data.jdbc.repository;

import org.springframework.data.jdbc.support.RowMapperOrResultsetExtractor;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.lang.Nullable;

/**
 * Configures a {@link org.springframework.jdbc.core.RowMapper} or a {@link ResultSetExtractor} for each type to be used
 * for extracting entities of that type from a {@link java.sql.ResultSet}.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 */
public interface QueryMappingConfiguration {

	@Nullable
	<T> RowMapperOrResultsetExtractor<?> getMapperOrExtractor(Class<T> type);

	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	QueryMappingConfiguration EMPTY = new QueryMappingConfiguration() {

		@Override
		public <T> RowMapperOrResultsetExtractor<?> getMapperOrExtractor(Class<T> type) {
			return null;
		}

	};

}
