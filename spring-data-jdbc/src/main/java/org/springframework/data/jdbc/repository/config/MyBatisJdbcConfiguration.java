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
package org.springframework.data.jdbc.repository.config;

import java.util.Optional;

import org.apache.ibatis.session.SqlSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.mybatis.MyBatisDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Configuration class tweaking Spring Data JDBC to use a {@link MyBatisDataAccessStrategy} instead of the default one.
 *
 * @author Oliver Drotbohm
 * @author Mikhail Polivakha
 * @since 1.1
 */
@Configuration(proxyBeanMethods = false)
public class MyBatisJdbcConfiguration extends AbstractJdbcConfiguration {

	private @Autowired SqlSession session;

	private @Autowired Optional<QueryMappingConfiguration> queryMappingConfiguration;

	@Bean
	@Override
	public DataAccessStrategy dataAccessStrategyBean(NamedParameterJdbcOperations operations, JdbcConverter jdbcConverter,
			JdbcMappingContext context, Dialect dialect) {

		return MyBatisDataAccessStrategy.createCombinedAccessStrategy(context, jdbcConverter, operations, session, dialect, queryMappingConfiguration.orElse(QueryMappingConfiguration.EMPTY));
	}
}
