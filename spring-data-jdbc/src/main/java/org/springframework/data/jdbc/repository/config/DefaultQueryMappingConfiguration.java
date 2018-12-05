package org.springframework.data.jdbc.repository.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.data.jdbc.support.RowMapperOrResultsetExtractor;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A {@link QueryMappingConfiguration} that allows for registration of {@link RowMapper}s and
 * {@link ResultSetExtractor}s via a fluent Api.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 */
public class DefaultQueryMappingConfiguration implements QueryMappingConfiguration {

	private Map<Class<?>, RowMapperOrResultsetExtractor<?>> mappers = new LinkedHashMap<>();

	@Nullable
	public <T> RowMapperOrResultsetExtractor<? extends T> getMapperOrExtractor(Class<T> type) {

		Assert.notNull(type, "Type must not be null");

		RowMapperOrResultsetExtractor<?> candidate = mappers.get(type);

		if (candidate == null) {

			for (Map.Entry<Class<?>, RowMapperOrResultsetExtractor<?>> entry : mappers.entrySet()) {

				if (type.isAssignableFrom(entry.getKey())) {
					candidate = entry.getValue();
				}
			}
		}
		return (RowMapperOrResultsetExtractor<? extends T>) candidate;
	}

	/**
	 * Registers a the given {@link RowMapper} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> DefaultQueryMappingConfiguration registerRowMapper(Class<T> type, RowMapper<? extends T> rowMapper) {

		mappers.put(type, RowMapperOrResultsetExtractor.of(rowMapper));

		return this;
	}

	/**
	 * Registers a the given {@link ResultSetExtractor} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> DefaultQueryMappingConfiguration registerResultSetExtractor(Class<T> type,
			ResultSetExtractor resultSetExtractor) {

		mappers.put(type, RowMapperOrResultsetExtractor.of(resultSetExtractor));

		return this;
	}
}
