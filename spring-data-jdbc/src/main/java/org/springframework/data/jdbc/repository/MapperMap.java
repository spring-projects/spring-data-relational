package org.springframework.data.jdbc.repository;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;

/**
 * A map from a type to a {@link ResultSetExtractor} to be used for extracting that type from {@link java.sql.ResultSet}s.
 *
 * @author Evgeni Dimitrov
 */
public interface MapperMap {
	@Nullable
	<T> ResultSetExtractor<? extends T> resultSetExtractorFor(Class<T> type);
	@Nullable
	<T> RowMapper<? extends T> rowMapperFor(Class<T> type);
	
	/**
	 * An immutable empty instance that will return {@literal null} for all arguments.
	 */
	MapperMap EMPTY = new MapperMap() {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.jdbc.repository.RowMapperMap#rowMapperFor(java.lang.Class)
		 */
		public <T> RowMapper<? extends T> rowMapperFor(Class<T> type) {
			return null;
		}

		@Override
		public <T> ResultSetExtractor<? extends T> resultSetExtractorFor(Class<T> type) {
			return null;
		}
	};

}
