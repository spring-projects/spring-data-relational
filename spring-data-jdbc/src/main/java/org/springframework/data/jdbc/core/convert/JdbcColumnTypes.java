/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.util.ClassUtils;

/**
 * Utility that determines the necessary type conversions between Java types used in the domain model and types
 * compatible with JDBC drivers.
 *
 * @author Jens Schauder
 * @author Mikhail Polivakha
 * @since 2.0
 */
public enum JdbcColumnTypes {

	INSTANCE {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		public Class<?> resolvePrimitiveType(Class<?> type) {

			return javaToDbType.entrySet().stream() //
					.filter(e -> e.getKey().isAssignableFrom(type)) //
					.map(e -> (Class<?>) e.getValue()) //
					.findFirst() //
					.orElseGet(() -> (Class) ClassUtils.resolvePrimitiveIfNecessary(type));
		}
	};

	private static final Map<Class<?>, Class<?>> javaToDbType = new LinkedHashMap<>();

	static {

		javaToDbType.put(Enum.class, String.class);
		javaToDbType.put(ZonedDateTime.class, OffsetDateTime.class);
		javaToDbType.put(OffsetDateTime.class, OffsetDateTime.class);
		javaToDbType.put(LocalDateTime.class, LocalDateTime.class);
		javaToDbType.put(Temporal.class, Timestamp.class);
	}

	public abstract Class<?> resolvePrimitiveType(Class<?> type);
}
