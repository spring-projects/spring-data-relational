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
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.H2Dialect;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.repository.Repository;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Test extracting the SQL from a repository method call and performing assertions on it.
 *
 * @author Jens Schauder
 */
@ExtendWith(MockitoExtension.class)
public class SqlInspectingR2dbcRepositoryUnitTests {

	R2dbcConverter r2dbcConverter = new MappingR2dbcConverter(new R2dbcMappingContext());

	DatabaseClient databaseClient;
	StatementRecorder recorder = StatementRecorder.newInstance();
	ReactiveDataAccessStrategy dataAccessStrategy = new DefaultReactiveDataAccessStrategy(H2Dialect.INSTANCE);


	@BeforeEach
	@SuppressWarnings("unchecked")
	public void before() {

		databaseClient = DatabaseClient.builder().connectionFactory(recorder)
				.bindMarkers(H2Dialect.INSTANCE.getBindMarkersFactory()).build();

	}

	@Test // GH-1856
	public void replacesSpelExpressionInQuery() {

		recorder.addStubbing(SqlInspectingR2dbcRepositoryUnitTests::isSelect, List.of());

		R2dbcRepositoryFactory factory = new R2dbcRepositoryFactory(databaseClient, dataAccessStrategy);
		MyPersonRepository repository = factory.getRepository(MyPersonRepository.class);

		assertThat(repository).isNotNull();

		repository.findBySpel().block(Duration.ofMillis(100));

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(SqlInspectingR2dbcRepositoryUnitTests::isSelect);

		assertThat(statement.getSql()).isEqualTo("select * from PERSONx");
	}

	private static boolean isSelect(String sql) {
		return sql.toLowerCase().startsWith("select");
	}

	interface MyPersonRepository extends Repository<Person, Long> {
		@Query("select * from #{#tableName +'x'}")
		Mono<Person> findBySpel();
	}

	static class Person {
		@Id long id;
	}
}
