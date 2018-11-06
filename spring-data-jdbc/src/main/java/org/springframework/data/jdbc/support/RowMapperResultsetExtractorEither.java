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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((resultSetExtractor == null) ? 0 : resultSetExtractor.hashCode());
		result = prime * result + ((rowMapper == null) ? 0 : rowMapper.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		RowMapperResultsetExtractorEither other = (RowMapperResultsetExtractorEither) obj;
		if (resultSetExtractor == null) {
			if (other.resultSetExtractor != null) return false;
		} else {
			if (!resultSetExtractor.equals(other.resultSetExtractor)) return false;
		}
		if (rowMapper == null) {
			if (other.rowMapper != null) return false;
		} else {
			if (!rowMapper.equals(other.rowMapper)) return false;
		}
		return true;
	}
	
}
