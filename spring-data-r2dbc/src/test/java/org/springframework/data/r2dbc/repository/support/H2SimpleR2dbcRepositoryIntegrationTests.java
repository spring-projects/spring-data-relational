/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.domain.Persistable;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.RelationalManagedTypes;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link SimpleR2dbcRepository} against H2.
 *
 * @author Mark Paluch
 * @author Greg Turnquist
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class H2SimpleR2dbcRepositoryIntegrationTests extends AbstractSimpleR2dbcRepositoryIntegrationTests {

	@Autowired private R2dbcEntityTemplate entityTemplate;

	@Autowired private WithIdentifierConversion withIdentifierConversion;

	@Autowired private RelationalMappingContext mappingContext;

	@Configuration
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}

		@Bean
		WithIdentifierConversion withIdentifierConversion(R2dbcEntityOperations operations) {
			return new R2dbcRepositoryFactory(operations).getRepository(WithIdentifierConversion.class);
		}

		@Override
		public R2dbcMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy,
				R2dbcCustomConversions r2dbcCustomConversions, RelationalManagedTypes r2dbcManagedTypes) {

			R2dbcMappingContext context = super.r2dbcMappingContext(namingStrategy, r2dbcCustomConversions,
					r2dbcManagedTypes);
			context.setForceQuote(false);

			return context;
		}

		@Override
		protected List<Object> getCustomConverters() {
			return List.of(LongToLongIdentifierConverter.INSTANCE, LongIdentifierToLongConverter.INSTANCE,
					IntegerToLongIdentifierConverter.INSTANCE);
		}
	}

	@Override
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	@Override
	protected String getCreateTableStatement() {
		return H2TestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}

	@Override
	void dropTables(JdbcOperations jdbc) {
		super.dropTables(jdbc);
		this.jdbc.execute("DROP TABLE IF EXISTS always_new");
		this.jdbc.execute("DROP TABLE IF EXISTS with_converted_identifier");
	}

	@Override
	void createTables(JdbcOperations jdbc) {
		super.createTables(jdbc);

		this.jdbc.execute("CREATE TABLE always_new (\n" //
				+ "    id          integer PRIMARY KEY,\n" //
				+ "    name        varchar(255) NOT NULL\n" //
				+ ");");

		this.jdbc.execute("CREATE TABLE with_converted_identifier (\n" //
				+ "    id          serial PRIMARY KEY,\n" //
				+ "    name        varchar(255) NOT NULL\n" //
				+ ");");
	}

	@Test // GH-90
	void shouldInsertNewObjectWithGivenId() {

		RelationalEntityInformation<AlwaysNew, Long> entityInformation = new MappingRelationalEntityInformation<>(
				(RelationalPersistentEntity<AlwaysNew>) mappingContext.getRequiredPersistentEntity(AlwaysNew.class));

		SimpleR2dbcRepository<AlwaysNew, Long> repository = new SimpleR2dbcRepository<>(entityInformation, entityTemplate,
				entityTemplate.getConverter());

		AlwaysNew alwaysNew = new AlwaysNew(9999L, "SCHAUFELRADBAGGER");

		repository.save(alwaysNew) //
				.as(StepVerifier::create) //
				.consumeNextWith( //
						actual -> assertThat(actual.getId()).isEqualTo(9999) //
				).verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM always_new");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsKey("id");
	}

	@Test // GH-232, GH-2176
	void updateShouldNotFailIfRowDoesNotExist() {

		LegoSet legoSet = new LegoSet(9999, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // GH-2225
	void findAllByIdWithIdConverter() {

		List<WithConvertedIdentifier> result = new ArrayList<>();
		this.withIdentifierConversion
				.saveAll(List.of(new WithConvertedIdentifier("one"), new WithConvertedIdentifier("two"),
						new WithConvertedIdentifier("three")))
				.as(StepVerifier::create) //
				.recordWith(() -> result) //
				.expectNextCount(3) //
				.verifyComplete();

		assertThat(result).hasSize(3);
		withIdentifierConversion.findAllById(result.stream().map(WithConvertedIdentifier::getId).toList())
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // GH-2225
	void deleteAllByIdWithIdConverter() {

		List<WithConvertedIdentifier> result = new ArrayList<>();
		this.withIdentifierConversion
				.saveAll(List.of(new WithConvertedIdentifier("one"), new WithConvertedIdentifier("two"),
						new WithConvertedIdentifier("three")))
				.as(StepVerifier::create) //
				.recordWith(() -> result) //
				.expectNextCount(3) //
				.verifyComplete();

		assertThat(result).hasSize(3);
		withIdentifierConversion.deleteAllById(result.stream().map(WithConvertedIdentifier::getId).toList())
				.as(StepVerifier::create) //
				.verifyComplete();

		withIdentifierConversion.count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // GH-2225
	void deleteAllWithIdConverter() {

		List<WithConvertedIdentifier> result = new ArrayList<>();
		this.withIdentifierConversion
				.saveAll(List.of(new WithConvertedIdentifier("one"), new WithConvertedIdentifier("two"),
						new WithConvertedIdentifier("three")))
				.as(StepVerifier::create) //
				.recordWith(() -> result) //
				.expectNextCount(3) //
				.verifyComplete();

		assertThat(result).hasSize(3);
		withIdentifierConversion.deleteAll(result) //
				.as(StepVerifier::create) //
				.verifyComplete();

		withIdentifierConversion.count().as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	static class AlwaysNew implements Persistable<Long> {

		@Id Long id;
		String name;

		public AlwaysNew(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public Long getId() {
			return id;
		}

		@Override
		public boolean isNew() {
			return true;
		}
	}

	record LongIdentifier(Long id) {
	}

	@WritingConverter
	enum LongIdentifierToLongConverter implements Converter<LongIdentifier, Long> {

		INSTANCE;

		@Override
		public Long convert(LongIdentifier source) {
			return source.id;
		}
	}

	@ReadingConverter
	enum LongToLongIdentifierConverter implements Converter<Long, LongIdentifier> {

		INSTANCE;

		@Override
		public LongIdentifier convert(Long source) {
			return new LongIdentifier(source);
		}
	}

	@ReadingConverter
	enum IntegerToLongIdentifierConverter implements Converter<Integer, LongIdentifier> {

		INSTANCE;

		@Override
		public LongIdentifier convert(Integer source) {
			return new LongIdentifier(source.longValue());
		}
	}

	public static class WithConvertedIdentifier {

		@Id LongIdentifier id;
		String name;

		public WithConvertedIdentifier() {}

		public WithConvertedIdentifier(String name) {
			this.name = name;
		}

		public LongIdentifier getId() {
			return id;
		}

		public void setId(LongIdentifier id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	public interface WithIdentifierConversion extends ReactiveCrudRepository<WithConvertedIdentifier, LongIdentifier> {}
}
