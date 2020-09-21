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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Data;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.MySqlTestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Integration tests for {@link DatabaseClient} against MySQL.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 */
public class MySqlDatabaseClientIntegrationTests extends AbstractDatabaseClientIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = MySqlTestSupport.database();

	@Override
	protected DataSource createDataSource() {
		return MySqlTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return MySqlTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return MySqlTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Test // gh-166
	public void considersBuiltInConverters() {

		ConnectionFactory connectionFactory = createConnectionFactory();
		JdbcTemplate jdbc = createJdbcTemplate(createDataSource());

		try {
			jdbc.execute("DROP TABLE boolean_mapping");
		} catch (DataAccessException e) {}
		jdbc.execute("CREATE TABLE boolean_mapping (id int, flag1 TINYINT, flag2 TINYINT)");

		BooleanMapping mapping = new BooleanMapping();
		mapping.setId(42);
		mapping.setFlag1(true);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(BooleanMapping.class).using(mapping).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		databaseClient.select().from(BooleanMapping.class).fetch().first() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.isFlag1()).isTrue()) //
				.verifyComplete();
	}

	@Test // gh-305
	public void shouldApplyCustomConverters() {

		ConnectionFactory connectionFactory = createConnectionFactory();
		JdbcTemplate jdbc = createJdbcTemplate(createDataSource());
		ReactiveDataAccessStrategy strategy = new DefaultReactiveDataAccessStrategy(MySqlDialect.INSTANCE,
				Arrays.asList(UuidToStringConverter.INSTANCE, StringToUuidConverter.INSTANCE));

		try {
			jdbc.execute("DROP TABLE uuid_type");
		} catch (DataAccessException e) {}
		jdbc.execute("CREATE TABLE uuid_type (id varchar(255), uuid_value varchar(255))");

		UuidType uuidType = new UuidType();
		uuidType.setId(UUID.randomUUID());
		uuidType.setUuidValue(UUID.randomUUID());

		DatabaseClient databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory)
				.dataAccessStrategy(strategy).build();

		databaseClient.insert().into(UuidType.class).using(uuidType).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		databaseClient.select().from(UuidType.class).matching(Criteria.where("id").is(uuidType.getId())) //
				.fetch().first() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getUuidValue()).isEqualTo(uuidType.getUuidValue())) //
				.verifyComplete();

		uuidType.setUuidValue(null);
		databaseClient.update().table(UuidType.class).using(uuidType).then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		databaseClient.execute("SELECT * FROM uuid_type WHERE id = ?") //
				.bind(0, uuidType.getId()) //
				.as(UuidType.class) //
				.fetch().first() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getUuidValue()).isNull()) //
				.verifyComplete();

		databaseClient.execute("SELECT * FROM uuid_type WHERE id in (:ids)") //
				.bind("ids", Collections.singleton(uuidType.getId())) //
				.as(UuidType.class) //
				.fetch().first() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getUuidValue()).isNull()) //
				.verifyComplete();
	}

	@Table("boolean_mapping")
	@Data
	static class BooleanMapping {

		int id;
		boolean flag1;
		boolean flag2;
	}

	@Table("uuid_type")
	@Data
	static class UuidType {

		@Id UUID id;
		UUID uuidValue;
	}

	@WritingConverter
	enum UuidToStringConverter implements Converter<UUID, String> {
		INSTANCE;

		@Override
		public String convert(UUID uuid) {
			return uuid.toString();
		}
	}

	@ReadingConverter
	enum StringToUuidConverter implements Converter<String, UUID> {
		INSTANCE;

		@Override
		public UUID convert(String value) {
			return UUID.fromString(value);
		}
	}

}
