/*
 * Copyright 2021 the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.relational.core.dialect.Db2Dialect;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * {@link WritingConverter} from {@link OffsetDateTime} to {@link Timestamp}.
 * The conversion preserves the {@link java.time.Instant} represented by {@link OffsetDateTime}
 *
 * @author Jens Schauder
 * @since 2.3
 */
@WritingConverter
enum OffsetDateTimeToTimestampConverter implements Converter<OffsetDateTime, Timestamp> {

	INSTANCE;
	@Override
	public Timestamp convert(OffsetDateTime source) {
		return Timestamp.from(source.toInstant());
	}
}
