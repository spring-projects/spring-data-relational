package org.springframework.data.jdbc.support;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
/**
 * Represents either a RowMapper or a ResultSetExtractor
 *
 * @author Evgeni Dimitrov
 */
public class RowMapperResultsetExtractorEither<T> {
	private final RowMapper<T> rowMapper;
	private final ResultSetExtractor<T> resultSetExtractor;
	
	private RowMapperResultsetExtractorEither(RowMapper<T> rowMapper, ResultSetExtractor<T> resultSetExtractor) {
		this.rowMapper = rowMapper;
		this.resultSetExtractor = resultSetExtractor;
	}
	
	public static RowMapperResultsetExtractorEither<?> of(RowMapper<?> rowMapper) {
		return new RowMapperResultsetExtractorEither<>(rowMapper, null);
	}
	
	public boolean isRowMapper() {
		return this.rowMapper != null;
	}
	
	public RowMapper<?> rowMapper() {
		return this.rowMapper;
	}
	 
	public static RowMapperResultsetExtractorEither<?> of(ResultSetExtractor<?> resultSetExtractor) {
		return new RowMapperResultsetExtractorEither<>(null, resultSetExtractor);
	}
	
	public boolean isResultSetExtractor() {
		return this.resultSetExtractor != null;
	}
	
	public ResultSetExtractor<?> resultSetExtractor() {
		return this.resultSetExtractor;
	}
	
	@Override
	public String toString() {
		return String.format("RowMapperResultsetExtractorEither[%s]", this.rowMapper != null ? this.rowMapper : this.resultSetExtractor);
	}
	
}
