/*
 * Copyright 2018-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.convert;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.function.BiFunction;

/**
 * Maps a {@link io.r2dbc.spi.Row} to an entity of type {@code T}, including entities referenced.
 *
 * @author Mark Paluch
 * @author Ryland Degnan
 */
public class EntityRowMapper<T> implements BiFunction<Row, RowMetadata, T> {

	private final Class<T> typeRoRead;
	private final R2dbcConverter converter;

	public EntityRowMapper(Class<T> typeRoRead, R2dbcConverter converter) {

		this.typeRoRead = typeRoRead;
		this.converter = converter;
	}

	@Override
	public T apply(Row row, RowMetadata metadata) {
		return converter.read(typeRoRead, row, metadata);
	}
}
