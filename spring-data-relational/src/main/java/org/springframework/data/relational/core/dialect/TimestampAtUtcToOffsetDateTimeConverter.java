/*
 * Copyright 2021-2024 the original author or authors.
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
package org.springframework.data.relational.core.dialect;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

/**
 * A reading convert to convert {@link Timestamp} to {@link OffsetDateTime}. For the conversion the {@link Timestamp}
 * gets considered to be at UTC and the result of the conversion will have an offset of 0 and represent the same
 * instant.
 *
 * @author Jens Schauder
 * @since 2.3
 */
@ReadingConverter
enum TimestampAtUtcToOffsetDateTimeConverter implements Converter<Timestamp, OffsetDateTime> {

	INSTANCE;

	@Override
	public OffsetDateTime convert(Timestamp timestamp) {
		return OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneId.of("UTC"));
	}
}
