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
package org.springframework.data.relational.core.dialect;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.relational.core.sql.IdentifierProcessing;

/**
 * A SQL dialect for MariaDb.
 *
 * @author Jens Schauder
 * @since 2.3
 */
public class MariaDbDialect extends MySqlDialect {

	public MariaDbDialect(IdentifierProcessing identifierProcessing) {
		super(identifierProcessing);
	}

	@Override
	public Collection<Object> getConverters() {
		return Arrays.asList(
				TimestampAtUtcToOffsetDateTimeConverter.INSTANCE,
				NumberToBooleanConverter.INSTANCE);
	}
}
