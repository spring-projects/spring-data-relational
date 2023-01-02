/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.data.relational.core.dialect.Db2Dialect;
import org.springframework.data.relational.core.dialect.H2Dialect;

/**
 * {@link Db2Dialect} that registers JDBC specific converters.
 *
 * @author Jens Schauder
 * @author Christoph Strobl
 * @since 2.3
 * @deprecated This is only used for H2 1.x support which will be dropped with the next major release. Use H2Dialect
 *             instead.
 */
@Deprecated
public class JdbcH2Dialect extends H2Dialect {

	public static JdbcH2Dialect INSTANCE = new JdbcH2Dialect();

	protected JdbcH2Dialect() {}

	@Override
	public Collection<Object> getConverters() {

		final Collection<Object> originalConverters = super.getConverters();

		if (isH2belowVersion2()) {

			List<Object> converters = new ArrayList<>(originalConverters);
			converters.add(H2TimestampWithTimeZoneToOffsetDateTimeConverter.INSTANCE);
			return converters;
		}

		return originalConverters;
	}

	static boolean isH2belowVersion2() {

		try {

			JdbcH2Dialect.class.getClassLoader().loadClass("org.h2.api.TimestampWithTimeZone");
			return true;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}
}
