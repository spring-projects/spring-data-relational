/*
 * Copyright 2018-2024 the original author or authors.
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

import static io.netty.buffer.ByteBufUtil.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.relational.core.query.Criteria.*;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.codec.Box;
import io.r2dbc.postgresql.codec.Circle;
import io.r2dbc.postgresql.codec.EnumCodec;
import io.r2dbc.postgresql.codec.Interval;
import io.r2dbc.postgresql.codec.Line;
import io.r2dbc.postgresql.codec.Lseg;
import io.r2dbc.postgresql.codec.Path;
import io.r2dbc.postgresql.codec.Point;
import io.r2dbc.postgresql.codec.Polygon;
import io.r2dbc.postgresql.extension.CodecRegistrar;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
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
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Integration tests for PostgreSQL-specific features such as array support.
 *
 * @author Mark Paluch
 */
public class PostgresIntegrationTests extends R2dbcIntegrationTestSupport {

	@RegisterExtension public static final ExternalDatabase database = PostgresTestSupport.database();

	private final DataSource dataSource = PostgresTestSupport.createDataSource(database);
	private final ConnectionFactory connectionFactory = PostgresTestSupport.createConnectionFactory(database);
	private final JdbcTemplate template = createJdbcTemplate(dataSource);
	private final DatabaseClient client = DatabaseClient.create(connectionFactory);

	@BeforeEach
	void before() {

		template.execute("DROP TABLE IF EXISTS with_arrays");
		template.execute("CREATE TABLE with_arrays (" //
				+ "id serial PRIMARY KEY," //
				+ "boxed_array INT[]," //
				+ "primitive_array INT[]," //
				+ "multidimensional_array INT[]," //
				+ "collection_array INT[][])");

		template.execute("DROP TABLE IF EXISTS with_blobs");
		template.execute("CREATE TABLE with_blobs (" //
				+ "id serial PRIMARY KEY," //
				+ "byte_array bytea," //
				+ "byte_buffer bytea," //
				+ "byte_blob bytea)");
	}

	@Test // gh-411
	void shouldWriteAndReadEnumValuesUsingDriverInternals() {

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

	private static class StateConverter extends EnumWriteSupport<State> {

	}

	@Test // gh-423
	void shouldReadAndWriteGeoTypes() {

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

	@Test // gh-573
	void shouldReadAndWriteInterval() {

		EntityWithInterval entityWithInterval = new EntityWithInterval();
		entityWithInterval.interval = Interval.of(Duration.ofHours(3));

		template.execute("DROP TABLE IF EXISTS with_interval");
		template.execute("CREATE TABLE with_interval (" //
				+ "id serial PRIMARY KEY," //
				+ "interval INTERVAL" //
				+ ")");

		R2dbcEntityTemplate template = new R2dbcEntityTemplate(client,
				new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));

		template.insert(entityWithInterval).thenMany(template.select(Query.empty(), EntityWithInterval.class)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.interval).isEqualTo(entityWithInterval.interval);
				}).verifyComplete();
	}

	@Test // gh-1408
	void shouldReadAndWriteBlobs() {

		R2dbcEntityTemplate template = new R2dbcEntityTemplate(client,
				new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE));

		WithBlobs withBlobs = new WithBlobs();
		byte[] content = "123ä".getBytes(StandardCharsets.UTF_8);

		withBlobs.byteArray = content;
		withBlobs.byteBuffer = ByteBuffer.wrap(content);
		withBlobs.byteBlob = Blob.from(Mono.just(ByteBuffer.wrap(content)));

		template.insert(withBlobs) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.selectOne(Query.empty(), WithBlobs.class) //
				.flatMap(it -> {
					return Flux.from(it.byteBlob.stream()).last().map(blob -> {
						it.byteBlob = Blob.from(Mono.just(blob));
						return it;
					});
				}).as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					CompletableFuture<byte[]> cf = Mono.from(actual.byteBlob.stream()).map(Unpooled::wrappedBuffer)
							.map(ByteBufUtil::getBytes).toFuture();
					assertThat(actual.byteArray).isEqualTo(content);
					assertThat(getBytes(Unpooled.wrappedBuffer(actual.byteBuffer))).isEqualTo(content);
					assertThat(cf.join()).isEqualTo(content);
				}).verifyComplete();

		template.selectOne(Query.empty(), WithBlobs.class)
				.doOnNext(it -> it.byteArray = "foo".getBytes(StandardCharsets.UTF_8)).flatMap(template::update) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		template.selectOne(Query.empty(), WithBlobs.class) //
				.flatMap(it -> {
					return Flux.from(it.byteBlob.stream()).last().map(blob -> {
						it.byteBlob = Blob.from(Mono.just(blob));
						return it;
					});
				}).as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					CompletableFuture<byte[]> cf = Mono.from(actual.byteBlob.stream()).map(Unpooled::wrappedBuffer)
							.map(ByteBufUtil::getBytes).toFuture();
					assertThat(actual.byteArray).isEqualTo("foo".getBytes(StandardCharsets.UTF_8));
					assertThat(getBytes(Unpooled.wrappedBuffer(actual.byteBuffer))).isEqualTo(content);
					assertThat(cf.join()).isEqualTo(content);
				}).verifyComplete();
	}

	static class EntityWithEnum {
		@Id long id;
		State myState;

		public EntityWithEnum(long id, State myState) {
			this.id = id;
			this.myState = myState;
		}
	}

	@Table("with_arrays")
	static class EntityWithArrays {

		@Id Integer id;
		Integer[] boxedArray;
		int[] primitiveArray;
		int[][] multidimensionalArray;
		List<Integer> collectionArray;

		public EntityWithArrays(Integer id, Integer[] boxedArray, int[] primitiveArray, int[][] multidimensionalArray,
				List<Integer> collectionArray) {
			this.id = id;
			this.boxedArray = boxedArray;
			this.primitiveArray = primitiveArray;
			this.multidimensionalArray = multidimensionalArray;
			this.collectionArray = collectionArray;
		}
	}

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

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			GeoType geoType = (GeoType) o;
			return Objects.equals(id, geoType.id) && Objects.equals(thePoint, geoType.thePoint) && Objects.equals(theBox, geoType.theBox) && Objects.equals(theCircle, geoType.theCircle) && Objects.equals(theLine, geoType.theLine) && Objects.equals(theLseg, geoType.theLseg) && Objects.equals(thePath, geoType.thePath) && Objects.equals(thePolygon, geoType.thePolygon) && Objects.equals(springDataBox, geoType.springDataBox) && Objects.equals(springDataCircle, geoType.springDataCircle) && Objects.equals(springDataPoint, geoType.springDataPoint) && Objects.equals(springDataPolygon, geoType.springDataPolygon);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, thePoint, theBox, theCircle, theLine, theLseg, thePath, thePolygon, springDataBox, springDataCircle, springDataPoint, springDataPolygon);
		}
	}

	@Table("with_interval")
	static class EntityWithInterval {

		@Id Integer id;

		Interval interval;

	}

	@Table("with_blobs")
	static class WithBlobs {

		@Id Integer id;

		byte[] byteArray;
		ByteBuffer byteBuffer;
		Blob byteBlob;

	}
}
