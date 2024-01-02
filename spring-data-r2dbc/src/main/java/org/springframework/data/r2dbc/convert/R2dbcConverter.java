/*
 * Copyright 2019-2024 the original author or authors.
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

import io.r2dbc.spi.Readable;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import java.util.function.BiFunction;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.dialect.ArrayColumns;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;

/**
 * Central R2DBC specific converter interface.
 *
 * @author Mark Paluch
 * @see EntityReader
 */
public interface R2dbcConverter
		extends EntityReader<Object, Row>, EntityWriter<Object, OutboundRow>, RelationalConverter {

	/**
	 * Returns the underlying {@link ConversionService} used by the converter.
	 *
	 * @return never {@literal null}.
	 */
	@Override
	ConversionService getConversionService();

	/**
	 * Convert a {@code value} into an array representation according to {@link ArrayColumns}.
	 *
	 * @param arrayColumns dialect-specific array handling configuration.
	 * @param property
	 * @param value
	 * @return
	 */
	Object getArrayValue(ArrayColumns arrayColumns, RelationalPersistentProperty property, Object value);

	/**
	 * Return the target type for a value considering registered converters.
	 *
	 * @param valueType must not be {@literal null}.
	 * @return
	 * @since 1.1
	 */
	Class<?> getTargetType(Class<?> valueType);

	/**
	 * Return whether the {@code type} is a simple type. Simple types are database primitives or types with a custom
	 * mapping strategy.
	 *
	 * @param type the type to inspect, must not be {@literal null}.
	 * @return {@literal true} if the type is a simple one.
	 * @see org.springframework.data.mapping.model.SimpleTypeHolder
	 * @since 1.2
	 */
	boolean isSimpleType(Class<?> type);

	/**
	 * Returns a {@link java.util.function.Function} that populates the id property of the {@code object} from a
	 * {@link Row}.
	 *
	 * @param object must not be {@literal null}.
	 * @return
	 */
	<T> BiFunction<Row, RowMetadata, T> populateIdIfNecessary(T object);

	/**
	 * Reads the given source into the given type.
	 *
	 * @param type they type to convert the given source to.
	 * @param source the source to create an object of the given type from.
	 * @param metadata the {@link RowMetadata}.
	 * @return
	 */
	<R> R read(Class<R> type, Row source, RowMetadata metadata);

	/**
	 * Create a flat {@link RowDocument} from a single {@link Readable Row or Stored Procedure output}.
	 *
	 * @param type the underlying entity type.
	 * @param row the row or stored procedure output to retrieve data from.
	 * @param metadata readable metadata.
	 * @return the {@link RowDocument} containing the data.
	 * @since 3.2
	 */
	RowDocument toRowDocument(Class<?> type, Readable row, Iterable<? extends ReadableMetadata> metadata);

}
