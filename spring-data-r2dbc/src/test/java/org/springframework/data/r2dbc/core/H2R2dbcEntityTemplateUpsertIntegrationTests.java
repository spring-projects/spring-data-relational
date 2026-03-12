/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;

import java.util.Collections;

import javax.sql.DataSource;

import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * H2-specific integration tests for {@link R2dbcEntityTemplate} upsert.
 *
 * @author Christoph Strobl
 */
public class H2R2dbcEntityTemplateUpsertIntegrationTests extends AbstractR2dbcEntityTemplateUpsertIntegrationTests {

	@Override
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return H2TestSupport.createConnectionFactory();
	}

	@Override
	protected String getCreateLegosetStatement() {
		return "CREATE TABLE legoset (" //
				+ "    id      bigint CONSTRAINT legoset_pk PRIMARY KEY," //
				+ "    name    varchar(255) NOT NULL," //
				+ "    manual  integer NULL" //
				+ ")";
	}

	@Override
	protected String getCreateWithInsertOnlyStatement() {
		return "CREATE TABLE with_insert_only (" //
				+ "    id           bigint CONSTRAINT with_insert_only_pk PRIMARY KEY," //
				+ "    insert_only  varchar(255) NULL" //
				+ ")";
	}

	@Override
	protected R2dbcEntityTemplate createEntityTemplate(ConnectionFactory connectionFactory) {

		R2dbcDialect dialect = DialectResolver.getDialect(connectionFactory);
		R2dbcCustomConversions customConversions = R2dbcCustomConversions.of(dialect, Collections.emptyList());

		R2dbcMappingContext context = new R2dbcMappingContext();
		context.setForceQuote(false);
		context.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		MappingR2dbcConverter converter = new MappingR2dbcConverter(context, customConversions);
		DefaultReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(dialect, converter);

		return new R2dbcEntityTemplate(DatabaseClient.create(connectionFactory), strategy);
	}
}
