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
import static org.springframework.data.relational.core.query.Criteria.*;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.codec.Box;
import io.r2dbc.postgresql.codec.Circle;
import io.r2dbc.postgresql.codec.EnumCodec;
import io.r2dbc.postgresql.codec.Line;
import io.r2dbc.postgresql.codec.Lseg;
import io.r2dbc.postgresql.codec.Path;
import io.r2dbc.postgresql.codec.Point;
import io.r2dbc.postgresql.codec.Polygon;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.convert.EnumWriteSupport;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.query.Query;
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

	@Test // gh-411
	public void shouldWriteAndReadEnumValuesUsingDriverInternals() {

		CodecRegistrar codecRegistrar = EnumCodec.builder().withEnum("state_enum", State.class).build();

		PostgresqlConnectionConfiguration configuration = PostgresqlConnectionConfiguration.builder() //
				.host(database.getHostname()) //
				.port(database.getPort()) //
				.database(database.getDatabase()) //
				.username(database.getUsername()) //
				.password(database.getPassword()) //
				.codecRegistrar(codecRegistrar).build();

		PostgresqlConnectionFactory connectionFactory = new PostgresqlConnectionFactory(configuration);

		try {
			template.execute("CREATE TYPE state_enum as enum ('Good', 'Bad')");
		} catch (DataAccessException e) {
			// ignore
		}
		template.execute("CREATE TABLE IF NOT EXISTS entity_with_enum (" //
				+ "id serial PRIMARY KEY," //
				+ "my_state state_enum)");
		template.execute("DELETE FROM entity_with_enum");

		ReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE,
				Collections.singletonList(new StateConverter()));
		R2dbcEntityTemplate entityTemplate = new R2dbcEntityTemplate(
				org.springframework.r2dbc.core.DatabaseClient.create(connectionFactory), strategy);

		entityTemplate.insert(new EntityWithEnum(0, State.Good)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		entityTemplate.select(Query.query(where("my_state").is(State.Good)), EntityWithEnum.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.myState).isEqualTo(State.Good);
				}).verifyComplete();
	}

	enum State {
		Good, Bad
	}

	static class StateConverter extends EnumWriteSupport<State> {

	}

	@Test // gh-423
	public void shouldReadAndWriteGeoTypes() {

		GeoType geoType = new GeoType();
		geoType.thePoint = Point.of(1, 2);
		geoType.theBox = Box.of(Point.of(3, 4), Point.of(1, 2));
		geoType.theCircle = Circle.of(1, 2, 3);
		geoType.theLine = Line.of(1, 2, 3, 4);
		geoType.theLseg = Lseg.of(Point.of(1, 2), Point.of(3, 4));
		geoType.thePath = Path.open(Point.of(1, 2), Point.of(3, 4));
		geoType.thePolygon = Polygon.of(Point.of(1, 2), Point.of(3, 4), Point.of(5, 6), Point.of(1, 2));
		geoType.springDataBox = new org.springframework.data.geo.Box(new org.springframework.data.geo.Point(3, 4),
				new org.springframework.data.geo.Point(1, 2));
		geoType.springDataCircle = new org.springframework.data.geo.Circle(1, 2, 3);
		geoType.springDataPoint = new org.springframework.data.geo.Point(1, 2);
		geoType.springDataPolygon = new org.springframework.data.geo.Polygon(new org.springframework.data.geo.Point(1, 2),
				new org.springframework.data.geo.Point(3, 4), new org.springframework.data.geo.Point(5, 6),
				new org.springframework.data.geo.Point(1, 2));

		template.execute("DROP TABLE IF EXISTS geo_type");
		template.execute("CREATE TABLE geo_type (" //
				+ "id serial PRIMARY KEY," //
				+ "the_point POINT," //
				+ "the_box BOX," //
				+ "the_circle CIRCLE," //
				+ "the_line LINE," //
				+ "the_lseg LSEG," //
				+ "the_path PATH," //
				+ "the_polygon POLYGON," //
				+ "spring_data_box BOX," //
				+ "spring_data_circle CIRCLE," //
				+ "spring_data_point POINT," //
				+ "spring_data_polygon POLYGON" //
				+ ")");

		R2dbcEntityTemplate template = new R2dbcEntityTemplate(client,
				new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));

		GeoType saved = template.insert(geoType).block();
		GeoType loaded = template.select(Query.empty(), GeoType.class) //
				.blockLast();

		assertThat(saved.id).isEqualTo(loaded.id);
		assertThat(saved.thePoint).isEqualTo(loaded.thePoint);
		assertThat(saved.theBox).isEqualTo(loaded.theBox);
		assertThat(saved.theCircle).isEqualTo(loaded.theCircle);
		assertThat(saved.theLine).isEqualTo(loaded.theLine);
		assertThat(saved.theLseg).isEqualTo(loaded.theLseg);
		assertThat(saved.thePath).isEqualTo(loaded.thePath);
		assertThat(saved.thePolygon).isEqualTo(loaded.thePolygon);
		assertThat(saved.springDataBox).isEqualTo(loaded.springDataBox);
		assertThat(saved.springDataCircle).isEqualTo(loaded.springDataCircle);
		assertThat(saved.springDataPoint).isEqualTo(loaded.springDataPoint);
		assertThat(saved.springDataPolygon).isEqualTo(loaded.springDataPolygon);
		assertThat(saved).isEqualTo(loaded);
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

	@Data
	@AllArgsConstructor
	static class EntityWithEnum {
		@Id long id;
		State myState;
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

	@Data
	static class GeoType {

		@Id Integer id;

		Point thePoint;
		Box theBox;
		Circle theCircle;
		Line theLine;
		Lseg theLseg;
		Path thePath;
		Polygon thePolygon;

		org.springframework.data.geo.Box springDataBox;
		org.springframework.data.geo.Circle springDataCircle;
		org.springframework.data.geo.Point springDataPoint;
		org.springframework.data.geo.Polygon springDataPolygon;
	}
}
