/*
 * Copyright 2019-2020 the original author or authors.
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

import java.sql.Array;
import java.sql.JDBCType;

import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.Assert;

/**
 * A {@link JdbcTypeFactory} that performs the conversion by utilizing
 * {@link JdbcOperations#execute(ConnectionCallback)}.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public class DefaultJdbcTypeFactory implements JdbcTypeFactory {

	private final JdbcOperations operations;

	/**
	 * Creates a new {@link DefaultJdbcTypeFactory}.
	 *
	 * @param operations must not be {@literal null}.
	 */
	public DefaultJdbcTypeFactory(JdbcOperations operations) {

		Assert.notNull(operations, "JdbcOperations must not be null");

		this.operations = operations;
	}

	@Override
	public Array createArray(Object[] value) {

		Assert.notNull(value, "Value must not be null.");

		Class<?> componentType = innermostComponentType(value);

		JDBCType jdbcType = JdbcUtil.jdbcTypeFor(componentType);
		Assert.notNull(jdbcType, () -> String.format("Couldn't determine JDBCType for %s", componentType));
		String typeName = jdbcType.getName();

		return operations.execute((ConnectionCallback<Array>) c -> c.createArrayOf(typeName, value));
	}

	private static Class<?> innermostComponentType(Object convertedValue) {

		Class<?> componentType = convertedValue.getClass();
		while (componentType.isArray()) {
			componentType = componentType.getComponentType();
		}
		return componentType;
	}
}
