package org.springframework.data.jdbc.repository.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

/**
 * A {@link QueryMappingConfiguration} that allows for registration of {@link RowMapper}s and
 * {@link ResultSetExtractor}s via a fluent Api.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 * @since 1.1
 */
public class DefaultQueryMappingConfiguration implements QueryMappingConfiguration {

	private final Map<Class<?>, RowMapper<?>> mappers = new LinkedHashMap<>();

	public <T extends @Nullable Object> @Nullable RowMapper<? extends T> getRowMapper(Class<T> type) {

		Assert.notNull(type, "Type must not be null");

		RowMapper<?> candidate = mappers.get(type);

		if (candidate == null) {

			for (Map.Entry<Class<?>, RowMapper<?>> entry : mappers.entrySet()) {

				if (type.isAssignableFrom(entry.getKey())) {
					candidate = entry.getValue();
				}
			}
		}
		return (RowMapper<? extends T>) candidate;
	}

	/**
	 * Registers a given {@link RowMapper} as to be used for the given type.
	 *
	 * @return this instance, so this can be used as a fluent interface.
	 */
	public <T> DefaultQueryMappingConfiguration registerRowMapper(Class<T> type, RowMapper<? extends T> rowMapper) {

		mappers.put(type, rowMapper);

		return this;
	}
}
