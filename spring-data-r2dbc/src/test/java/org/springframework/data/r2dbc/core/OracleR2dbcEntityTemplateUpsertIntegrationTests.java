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

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.OracleTestSupport;

/**
 * Oracle-specific integration tests for {@link R2dbcEntityTemplate} upsert.
 *
 * @author Christoph Strobl
 */
public class OracleR2dbcEntityTemplateUpsertIntegrationTests
		extends AbstractR2dbcEntityTemplateUpsertIntegrationTests {

	@RegisterExtension
	public static final ExternalDatabase database = OracleTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return OracleTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return OracleTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateLegosetStatement() {
		return "CREATE TABLE \"legoset\" (" //
				+ "    id      NUMBER(19) CONSTRAINT legoset_pk PRIMARY KEY," //
				+ "    name    VARCHAR2(255) NOT NULL," //
				+ "    manual  NUMBER(10) NULL" //
				+ ")";
	}

	@Override
	protected String getCreateWithInsertOnlyStatement() {
		return "CREATE TABLE \"with_insert_only\" (" //
				+ "    id           NUMBER(19) CONSTRAINT with_insert_only_pk PRIMARY KEY," //
				+ "    insert_only  VARCHAR2(255) NULL" //
				+ ")";
	}

	@Override
	protected String getDropLegosetStatement() {
		return "DROP TABLE \"legoset\"";
	}

	@Override
	protected String getDropWithInsertOnlyStatement() {
		return "DROP TABLE \"with_insert_only\"";
	}
}
