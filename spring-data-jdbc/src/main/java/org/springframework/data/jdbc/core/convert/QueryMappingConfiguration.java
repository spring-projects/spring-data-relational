package org.springframework.data.jdbc.core.convert;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * Configures a {@link org.springframework.jdbc.core.RowMapper} for each type to be used for extracting entities of that
 * type from a {@link java.sql.ResultSet}.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 * @since 1.1
 */
public interface QueryMappingConfiguration {

	@Nullable
	default <T> RowMapper<? extends T> getRowMapper(Class<T> type) {
		return null;
	}

	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	QueryMappingConfiguration EMPTY = new QueryMappingConfiguration() {

		@Override
		public <T> RowMapper<? extends T> getRowMapper(Class<T> type) {
			return null;
		}

	};

}
