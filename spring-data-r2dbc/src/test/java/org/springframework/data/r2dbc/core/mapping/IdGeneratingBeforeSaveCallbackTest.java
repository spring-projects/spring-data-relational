/*
 * Copyright 2020-2025 the original author or authors.
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

package org.springframework.data.r2dbc.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.MySqlDialect;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.Sequence;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests for {@link IdGeneratingBeforeSaveCallback}.
 *
 * @author Mikhail Polivakha
 */
class IdGeneratingBeforeSaveCallbackTest {

	@Test
	void testIdGenerationIsNotSupported() {
		R2dbcMappingContext r2dbcMappingContext = new R2dbcMappingContext();
		r2dbcMappingContext.getPersistentEntity(SimpleEntity.class);
		MySqlDialect dialect = MySqlDialect.INSTANCE;
		DatabaseClient databaseClient = mock(DatabaseClient.class);

		IdGeneratingBeforeSaveCallback callback = new IdGeneratingBeforeSaveCallback(r2dbcMappingContext, dialect,
				databaseClient);

		OutboundRow row = new OutboundRow("name", Parameter.from("my_name"));
		SimpleEntity entity = new SimpleEntity();
		Publisher<Object> publisher = callback.onBeforeSave(entity, row, SqlIdentifier.unquoted("simple_entity"));

		StepVerifier.create(publisher).expectNext(entity).expectComplete().verify();
		assertThat(row).hasSize(1); // id is not added
	}

	@Test
	void testEntityIsNotAnnotatedWithSequence() {
		R2dbcMappingContext r2dbcMappingContext = new R2dbcMappingContext();
		r2dbcMappingContext.getPersistentEntity(SimpleEntity.class);
		PostgresDialect dialect = PostgresDialect.INSTANCE;
		DatabaseClient databaseClient = mock(DatabaseClient.class);

		IdGeneratingBeforeSaveCallback callback = new IdGeneratingBeforeSaveCallback(r2dbcMappingContext, dialect,
				databaseClient);

		OutboundRow row = new OutboundRow("name", Parameter.from("my_name"));
		SimpleEntity entity = new SimpleEntity();
		Publisher<Object> publisher = callback.onBeforeSave(entity, row, SqlIdentifier.unquoted("simple_entity"));

		StepVerifier.create(publisher).expectNext(entity).expectComplete().verify();
		assertThat(row).hasSize(1); // id is not added
	}

	@Test
	void testIdGeneratedFromSequenceHappyPath() {
		R2dbcMappingContext r2dbcMappingContext = new R2dbcMappingContext();
		r2dbcMappingContext.getPersistentEntity(WithSequence.class);
		PostgresDialect dialect = PostgresDialect.INSTANCE;
		DatabaseClient databaseClient = mock(DatabaseClient.class, RETURNS_DEEP_STUBS);
		long generatedId = 1L;

		when(databaseClient.sql(Mockito.anyString()).map(Mockito.any(BiFunction.class)).one()).thenReturn(
				Mono.just(generatedId));

		IdGeneratingBeforeSaveCallback callback = new IdGeneratingBeforeSaveCallback(r2dbcMappingContext, dialect,
				databaseClient);

		OutboundRow row = new OutboundRow("name", Parameter.from("my_name"));
		WithSequence entity = new WithSequence();
		Publisher<Object> publisher = callback.onBeforeSave(entity, row, SqlIdentifier.unquoted("simple_entity"));

		StepVerifier.create(publisher).expectNext(entity).expectComplete().verify();
		assertThat(row).hasSize(2)
				.containsEntry(SqlIdentifier.unquoted("id"), Parameter.from(generatedId));
	}

	static class SimpleEntity {

		@Id
		private Long id;

		private String name;
	}

	static class WithSequence {

		@Id
		@Sequence(sequence = "seq_name")
		private Long id;

		private String name;
	}
}
