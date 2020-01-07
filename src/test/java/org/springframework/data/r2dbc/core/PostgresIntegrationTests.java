/*
 * Copyright 2018-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for PostgreSQL-specific features such as array support.
 *
 * @author Mark Paluch
 */
public class PostgresIntegrationTests extends R2dbcIntegrationTestSupport {

	@ClassRule public static final ExternalDatabase database = PostgresTestSupport.database();

	DataSource dataSource = PostgresTestSupport.createDataSource(database);
	ConnectionFactory connectionFactory = PostgresTestSupport.createConnectionFactory(database);
	JdbcTemplate template = createJdbcTemplate(dataSource);
	DatabaseClient client = DatabaseClient.create(connectionFactory);

	@Before
	public void before() {

		template.execute("DROP TABLE IF EXISTS with_arrays");
		template.execute("CREATE TABLE with_arrays (" //
				+ "id serial PRIMARY KEY," //
				+ "boxed_array INT[]," //
				+ "primitive_array INT[]," //
				+ "multidimensional_array INT[]," //
				+ "collection_array INT[][])");
	}

	@Test // gh-30
	public void shouldReadAndWritePrimitiveSingleDimensionArrays() {

		EntityWithArrays withArrays = new EntityWithArrays(null, null, new int[] { 1, 2, 3 }, null, null);

		insert(withArrays);
		selectAndAssert(actual -> {
			assertThat(actual.primitiveArray).containsExactly(1, 2, 3);
		});
	}

	@Test // gh-30
	public void shouldReadAndWriteBoxedSingleDimensionArrays() {

		EntityWithArrays withArrays = new EntityWithArrays(null, new Integer[] { 1, 2, 3 }, null, null, null);

		insert(withArrays);

		selectAndAssert(actual -> {

			assertThat(actual.boxedArray).containsExactly(1, 2, 3);

		});
	}

	@Test // gh-30
	public void shouldReadAndWriteConvertedDimensionArrays() {

		EntityWithArrays withArrays = new EntityWithArrays(null, null, null, null, Arrays.asList(5, 6, 7));

		insert(withArrays);

		selectAndAssert(actual -> {
			assertThat(actual.collectionArray).containsExactly(5, 6, 7);
		});
	}

	@Test // gh-30
	public void shouldReadAndWriteMultiDimensionArrays() {

		EntityWithArrays withArrays = new EntityWithArrays(null, null, null, new int[][] { { 1, 2, 3 }, { 4, 5, 6 } },
				null);

		insert(withArrays);

		selectAndAssert(actual -> {

			assertThat(actual.multidimensionalArray).hasSize(2);
			assertThat(actual.multidimensionalArray[0]).containsExactly(1, 2, 3);
			assertThat(actual.multidimensionalArray[1]).containsExactly(4, 5, 6);
		});

		client.update().table(EntityWithArrays.class).using(withArrays).then() //
				.as(StepVerifier::create).verifyComplete();
	}

	private void insert(EntityWithArrays object) {

		client.insert() //
				.into(EntityWithArrays.class) //
				.using(object) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	private void selectAndAssert(Consumer<? super EntityWithArrays> assertion) {

		client.select() //
				.from(EntityWithArrays.class).fetch() //
				.first() //
				.as(StepVerifier::create) //
				.consumeNextWith(assertion).verifyComplete();
	}

	@Table("with_arrays")
	@AllArgsConstructor
	static class EntityWithArrays {

		@Id Integer id;
		Integer[] boxedArray;
		int[] primitiveArray;
		int[][] multidimensionalArray;
		List<Integer> collectionArray;
	}
}
