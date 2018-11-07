package org.springframework.data.jdbc.repository.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.jdbc.repository.MapperMap;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

/**
 * A {@link MapperMap} that allows for registration of {@link RowMapper}s and {@link ResultSetExtractor}s via a fluent Api.
 *
 * @author Evgeni Dimitrov
 */
public class ConfigurableMapperMap implements MapperMap{
	private Map<Class<?>, Object> mappers = new LinkedHashMap<>();
	@Override
	public <T> ResultSetExtractor<? extends T> resultSetExtractorFor(Class<T> type) {
		Object candidate = getMapper(type);
		if(candidate != null && candidate instanceof ResultSetExtractor) {
			return (ResultSetExtractor) candidate;
		}
		return null;
	}

	@Override
	public <T> RowMapper<? extends T> rowMapperFor(Class<T> type) {
		Object candidate = getMapper(type);
		if(candidate != null && candidate instanceof RowMapper) {
			return (RowMapper) candidate;
		}
		return null;
	}

	private <T> Object getMapper(Class<T> type) {
		Assert.notNull(type, "Type must not be null");

		Object candidate = mappers.get(type);

		if (candidate == null) {

			for (Map.Entry<Class<?>, Object> entry : mappers.entrySet()) {

				if (type.isAssignableFrom(entry.getKey())) {
					candidate = entry.getValue();
				}
			}
		}
		return candidate;
	}

	/**
	 * Registers a the given {@link RowMapper} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> ConfigurableMapperMap registerRowMapper(Class<T> type, RowMapper<? extends T> rowMapper) {
		mappers.put(type, rowMapper);
		return this;
	}
	
	/**
	 * Registers a the given {@link ResultSetExtractor} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> ConfigurableMapperMap registerResultSetExtractor(Class<T> type, ResultSetExtractor resultSetExtractor) {
		mappers.put(type, resultSetExtractor);
		return this;
	}
}
