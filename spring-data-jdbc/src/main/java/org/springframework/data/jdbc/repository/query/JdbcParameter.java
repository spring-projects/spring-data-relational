/*
 * Copyright 2020-2025 the original author or authors.
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

package org.springframework.data.jdbc.repository.query;

import java.sql.SQLType;

import org.springframework.core.MethodParameter;
import org.springframework.data.jdbc.core.convert.JdbcColumnTypes;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.TypeInformation;

/**
 * Custom {@link Parameter} implementation.
 *
 * @author Mark Paluch
 * @author Chirag Tailor
 */
public class JdbcParameter extends RelationalParameters.RelationalParameter {

	private final SQLType sqlType;
	private final Lazy<SQLType> actualSqlType;

	/**
	 * Creates a new {@link RelationalParameters.RelationalParameter}.
	 *
	 * @param parameter must not be {@literal null}.
	 */
	public JdbcParameter(MethodParameter parameter, TypeInformation<?> domainType) {
		super(parameter, domainType);

		TypeInformation<?> typeInformation = getTypeInformation();

		sqlType = JdbcUtil.targetSqlTypeFor(JdbcColumnTypes.INSTANCE.resolvePrimitiveType(typeInformation.getType()));

		actualSqlType = Lazy.of(() -> JdbcUtil.targetSqlTypeFor(
				JdbcColumnTypes.INSTANCE.resolvePrimitiveType(typeInformation.getActualType().getType())));
	}

	public JdbcParameter(MethodParameter parameter, TypeInformation<?> domainType, SQLType sqlType,
			Lazy<SQLType> actualSqlType) {
		super(parameter, domainType);

		this.sqlType = sqlType;
		this.actualSqlType = actualSqlType;
	}

	public SQLType getSqlType() {
		return sqlType;
	}

	public SQLType getActualSqlType() {
		return actualSqlType.get();
	}
}
