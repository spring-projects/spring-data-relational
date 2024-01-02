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
package org.springframework.data.r2dbc.repository;

import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.mapping.event.AfterConvertCallback;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link LegoSetRepository} using {@link R2dbcRepositoryFactory} against H2.
 *
 * @author Mark Paluch
 * @author Zsombor Gegesy
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class H2R2dbcRepositoryIntegrationTests extends AbstractR2dbcRepositoryIntegrationTests {

	@Autowired private H2LegoSetRepository repository;
	@Autowired private IdOnlyEntityRepository idOnlyEntityRepository;
	@Autowired private AfterConvertCallbackRecorder recorder;

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(classes = {H2LegoSetRepository.class, IdOnlyEntityRepository.class}, type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}

		@Bean
		public AfterConvertCallbackRecorder afterConvertCallbackRecorder() {
			return new AfterConvertCallbackRecorder();
		}
	}

	@BeforeEach
	void setUp() {
		recorder.clear();
	}

	@Override
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return H2TestSupport.createConnectionFactory();
	}

	@Override
	protected String getCreateTableStatement() {
		return H2TestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}

	@Override
	protected Class<? extends LegoSetRepository> getRepositoryInterfaceType() {
		return H2LegoSetRepository.class;
	}

	@Test // gh-591
	void shouldFindItemsByManual() {
		super.shouldFindItemsByManual();
		assertThat(recorder.seenEntities).hasSize(1);
	}

	@Test // gh-591
	void shouldFindItemsByNameContains() {
		super.shouldFindItemsByNameContains();
		assertThat(recorder.seenEntities).hasSize(2);
	}

	@Test // gh-469
	void shouldSuppressNullValues() {
		repository.findMax("doo").as(StepVerifier::create).verifyComplete();
	}

	@Test // gh-235
	void shouldReturnUpdateCount() {

		shouldInsertNewItems();

		repository.updateManual(42).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // gh-235
	void shouldReturnUpdateCountAsDouble() {

		shouldInsertNewItems();

		repository.updateManualAndReturnDouble(42).as(StepVerifier::create).expectNext(2.0).verifyComplete();
	}

	@Test // gh-235
	void shouldReturnUpdateSuccess() {

		shouldInsertNewItems();

		repository.updateManualAndReturnBoolean(42).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // gh-235
	void shouldNotReturnUpdateCount() {

		shouldInsertNewItems();

		repository.updateManualAndReturnNothing(42).as(StepVerifier::create).verifyComplete();
	}

	@Test // gh-390
	void shouldInsertIdOnlyEntity() {

		this.jdbc.execute("CREATE TABLE ID_ONLY(id serial CONSTRAINT id_only_pk PRIMARY KEY)");

		IdOnlyEntity entity1 = new IdOnlyEntity();
		idOnlyEntityRepository.saveAll(Collections.singletonList(entity1))
			.as(StepVerifier::create) //
			.consumeNextWith( actual -> {
				assertThat(actual.getId()).isNotNull();
			}).verifyComplete();
	}

	@Test // gh-519
	void shouldReturnEntityThroughInterface() {

		shouldInsertNewItems();

		repository.findByName("SCHAUFELRADBAGGER").map(Buildable::getName).as(StepVerifier::create)
				.expectNext("SCHAUFELRADBAGGER").verifyComplete();
	}

	interface H2LegoSetRepository extends LegoSetRepository {

		Mono<Buildable> findByName(String name);

		@Query("SELECT MAX(manual) FROM legoset WHERE name = :name")
		Mono<Integer> findMax(String name);

		@Override
		@Query("SELECT name FROM legoset")
		Flux<Named> findAsProjection();

		@Override
		@Query("SELECT * FROM legoset WHERE manual = :manual")
		Mono<LegoSet> findByManual(int manual);

		@Override
		@Query("SELECT id FROM legoset")
		Flux<Integer> findAllIds();

		@Query("UPDATE legoset set manual = :manual")
		@Modifying
		Mono<Long> updateManual(int manual);

		@Query("UPDATE legoset set manual = :manual")
		@Modifying
		Mono<Boolean> updateManualAndReturnBoolean(int manual);

		@Query("UPDATE legoset set manual = :manual")
		@Modifying
		Mono<Void> updateManualAndReturnNothing(int manual);

		@Query("UPDATE legoset set manual = :manual")
		@Modifying
		Mono<Double> updateManualAndReturnDouble(int manual);
	}

	interface IdOnlyEntityRepository extends ReactiveCrudRepository<IdOnlyEntity, Integer> {}

	@Table("id_only")
	static class IdOnlyEntity {
		@Id
		Integer id;

		public IdOnlyEntity() {
		}

		public Integer getId() {
			return this.id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}

	static class AfterConvertCallbackRecorder implements AfterConvertCallback<LegoSet> {

		List<LegoSet> seenEntities = new ArrayList<>();

		@Override
		public Publisher<LegoSet> onAfterConvert(LegoSet entity, SqlIdentifier table) {
			seenEntities.add(entity);
			return Mono.just(entity);
		}

		public void clear() {
			seenEntities.clear();
		}
	}
}
