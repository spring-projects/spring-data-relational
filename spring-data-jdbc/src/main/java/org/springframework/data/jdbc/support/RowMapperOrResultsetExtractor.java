package org.springframework.data.jdbc.support;

import lombok.Value;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * Represents either a RowMapper or a ResultSetExtractor
 *
 * @author Evgeni Dimitrov
 * @author Jens Schauder
 */
@Value
public class RowMapperOrResultsetExtractor<T> {

	private final RowMapper<T> rowMapper;
	private final ResultSetExtractor<T> resultSetExtractor;

	private RowMapperOrResultsetExtractor(RowMapper<T> rowMapper, ResultSetExtractor<T> resultSetExtractor) {

		this.rowMapper = rowMapper;
		this.resultSetExtractor = resultSetExtractor;
	}

	public static <T> RowMapperOrResultsetExtractor<T> of(RowMapper<T> rowMapper) {
		return new RowMapperOrResultsetExtractor<>(rowMapper, null);
	}

	public static <T> RowMapperOrResultsetExtractor<T> of(ResultSetExtractor<T> resultSetExtractor) {
		return new RowMapperOrResultsetExtractor<>(null, resultSetExtractor);
	}

	public boolean isRowMapper() {
		return this.rowMapper != null;
	}

	public boolean isResultSetExtractor() {
		return this.resultSetExtractor != null;
	}

}
