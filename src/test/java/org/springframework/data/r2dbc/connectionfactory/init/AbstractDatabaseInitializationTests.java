/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory.init;

import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;

import org.springframework.core.io.ClassRelativeResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.data.r2dbc.core.DatabaseClient;

/**
 * Abstract test support for {@link DatabasePopulator}.
 *
 * @author Mark Paluch
 */
abstract class AbstractDatabaseInitializationTests {

	private final ClassRelativeResourceLoader resourceLoader = new ClassRelativeResourceLoader(getClass());
	ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

	@Test
	void scriptWithSingleLineCommentsAndFailedDrop() {

		databasePopulator.addScript(resource("db-schema-failed-drop-comments.sql"));
		databasePopulator.addScript(resource("db-test-data.sql"));
		databasePopulator.setIgnoreFailedDrops(true);

		runPopulator();

		assertUsersDatabaseCreated("Heisenberg");
	}

	private void runPopulator() {
		DatabasePopulatorUtils.execute(databasePopulator, getConnectionFactory()) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test
	void scriptWithStandardEscapedLiteral() {

		databasePopulator.addScript(defaultSchema());
		databasePopulator.addScript(resource("db-test-data-escaped-literal.sql"));

		runPopulator();

		assertUsersDatabaseCreated("'Heisenberg'");
	}

	@Test
	void scriptWithMySqlEscapedLiteral() {

		databasePopulator.addScript(defaultSchema());
		databasePopulator.addScript(resource("db-test-data-mysql-escaped-literal.sql"));

		runPopulator();

		assertUsersDatabaseCreated("\\$Heisenberg\\$");
	}

	@Test
	void scriptWithMultipleStatements() {

		databasePopulator.addScript(defaultSchema());
		databasePopulator.addScript(resource("db-test-data-multiple.sql"));

		runPopulator();

		assertUsersDatabaseCreated("Heisenberg", "Jesse");
	}

	@Test
	void scriptWithMultipleStatementsAndLongSeparator() {

		databasePopulator.addScript(defaultSchema());
		databasePopulator.addScript(resource("db-test-data-endings.sql"));
		databasePopulator.setSeparator("@@");

		runPopulator();

		assertUsersDatabaseCreated("Heisenberg", "Jesse");
	}

	abstract ConnectionFactory getConnectionFactory();

	Resource resource(String path) {
		return resourceLoader.getResource(path);
	}

	Resource defaultSchema() {
		return resource("db-schema.sql");
	}

	Resource usersSchema() {
		return resource("users-schema.sql");
	}

	void assertUsersDatabaseCreated(String... lastNames) {
		assertUsersDatabaseCreated(getConnectionFactory(), lastNames);
	}

	void assertUsersDatabaseCreated(ConnectionFactory connectionFactory, String... lastNames) {

		DatabaseClient client = DatabaseClient.create(connectionFactory);

		for (String lastName : lastNames) {

			client.execute("select count(0) from users where last_name = :name") //
					.bind("name", lastName) //
					.map((row, metadata) -> row.get(0)) //
					.first() //
					.map(it -> ((Number) it).intValue()) //
					.as(StepVerifier::create) //
					.expectNext(1).as("Did not find user with last name [" + lastName + "].") //
					.verifyComplete();
		}
	}
}
