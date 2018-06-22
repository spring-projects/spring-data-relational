/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;

import org.assertj.core.api.Condition;
import org.assertj.core.api.SoftAssertions;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests storing and retrieving various data types that are considered essential and that might need conversion to
 * something the database driver can handle.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
public class JdbcRepositoryPropertyConversionIntegrationTests {

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryPropertyConversionIntegrationTests.class;
		}

		@Bean
		DummyEntityRepository dummyEntityRepository() {
			return factory.getRepository(DummyEntityRepository.class);
		}

		@Bean
		ApplicationListener<?> applicationListener() {
			return (ApplicationListener<BeforeSaveEvent>) beforeInsert -> ((EntityWithColumnsRequiringConversions) beforeInsert
					.getEntity()).setIdTimestamp(getNow());
		}

	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired DummyEntityRepository repository;

	@Test // DATAJDBC-95
	public void saveAndLoadAnEntity() {

		EntityWithColumnsRequiringConversions entity = repository.save(createDummyEntity());

		assertThat(repository.findById(entity.getIdTimestamp())).hasValueSatisfying(it -> {
			SoftAssertions softly = new SoftAssertions();
			softly.assertThat(it.getIdTimestamp()).isEqualTo(entity.getIdTimestamp());
			softly.assertThat(it.getSomeEnum()).isEqualTo(entity.getSomeEnum());
			softly.assertThat(it.getBigDecimal()).isEqualTo(entity.getBigDecimal());
			softly.assertThat(it.isBool()).isEqualTo(entity.isBool());
			softly.assertThat(it.getBigInteger()).isEqualTo(entity.getBigInteger());
			softly.assertThat(it.getDate()).is(representingTheSameAs(entity.getDate()));
			softly.assertThat(it.getLocalDateTime()).isEqualTo(entity.getLocalDateTime());
			softly.assertAll();
		});
	}

	@Test // DATAJDBC-95
	public void existsById() {

		EntityWithColumnsRequiringConversions entity = repository.save(createDummyEntity());

		assertThat(repository.existsById(entity.getIdTimestamp())).isTrue();
	}

	@Test // DATAJDBC-95
	public void findAllById() {

		EntityWithColumnsRequiringConversions entity = repository.save(createDummyEntity());

		assertThat(repository.findAllById(Collections.singletonList(entity.getIdTimestamp()))).hasSize(1);
	}

	@Test // DATAJDBC-95
	public void deleteAll() {

		EntityWithColumnsRequiringConversions entity = repository.save(createDummyEntity());

		repository.deleteAll(singletonList(entity));

		assertThat(repository.findAll()).hasSize(0);
	}

	@Test // DATAJDBC-95
	public void deleteById() {

		EntityWithColumnsRequiringConversions entity = repository.save(createDummyEntity());

		repository.deleteById(entity.getIdTimestamp());

		assertThat(repository.findAll()).hasSize(0);
	}

	private static EntityWithColumnsRequiringConversions createDummyEntity() {

		EntityWithColumnsRequiringConversions entity = new EntityWithColumnsRequiringConversions();
		entity.setSomeEnum(SomeEnum.VALUE);
		entity.setBigDecimal(new BigDecimal("123456789012345678901234567890123456789012345678901234567890"));
		entity.setBool(true);
		// Postgres doesn't seem to be able to handle BigInts larger then a Long, since the driver reads them as Long
		entity.setBigInteger(BigInteger.valueOf(Long.MAX_VALUE));
		entity.setDate(Date.from(getNow().toInstant(ZoneOffset.UTC)));
		entity.setLocalDateTime(getNow());

		return entity;
	}

	// DATAJDBC-119
	private static LocalDateTime getNow() {
		return LocalDateTime.now().withNano(0);
	}

	private Condition<Date> representingTheSameAs(Date other) {

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		String expected = format.format(other);

		return new Condition<>(date -> format.format(date).equals(expected), expected);
	}

	interface DummyEntityRepository extends CrudRepository<EntityWithColumnsRequiringConversions, LocalDateTime> {}

	@Data
	static class EntityWithColumnsRequiringConversions {

		// ensures conversion on id querying
		@Id private LocalDateTime idTimestamp;

		boolean bool;

		SomeEnum someEnum;

		BigDecimal bigDecimal;

		BigInteger bigInteger;

		Date date;

		LocalDateTime localDateTime;

	}

	enum SomeEnum {
		VALUE
	}
}
