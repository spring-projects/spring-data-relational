/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.any;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.dialect.JdbcMySqlDialect;
import org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Sequence;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Unit tests for {@link IdGeneratingEntityCallback}
 *
 * @author Mikhail Polivakha
 * @author Mark Paluch
 */
@MockitoSettings(strictness = Strictness.LENIENT)
class IdGeneratingEntityCallbackTest {

	@Mock NamedParameterJdbcOperations operations;
	RelationalMappingContext relationalMappingContext;

	@BeforeEach
	void setUp() {

		relationalMappingContext = new RelationalMappingContext();
		relationalMappingContext
				.setSimpleTypeHolder(new SimpleTypeHolder(JdbcPostgresDialect.INSTANCE.simpleTypes(), true));
	}

	@Test // GH-1923
	void sequenceGenerationIsNotSupported() {

		NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class);

		IdGeneratingEntityCallback subject = new IdGeneratingEntityCallback(relationalMappingContext,
				JdbcMySqlDialect.INSTANCE, operations);

		EntityWithSequence processed = (EntityWithSequence) subject.onBeforeSave(new EntityWithSequence(),
				MutableAggregateChange.forSave(new EntityWithSequence()));

		assertThat(processed.id).isNull();
	}

	@Test // GH-1923
	void entityIsNotMarkedWithTargetSequence() {

		IdGeneratingEntityCallback subject = new IdGeneratingEntityCallback(relationalMappingContext,
				JdbcMySqlDialect.INSTANCE, operations);

		NoSequenceEntity processed = (NoSequenceEntity) subject.onBeforeSave(new NoSequenceEntity(),
				MutableAggregateChange.forSave(new NoSequenceEntity()));

		assertThat(processed.id).isNull();
	}

	@Test // GH-1923
	void entityIdIsPopulatedFromSequence() {

		long generatedId = 112L;
		when(operations.queryForObject(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
				.thenReturn(generatedId);

		IdGeneratingEntityCallback subject = new IdGeneratingEntityCallback(relationalMappingContext,
				JdbcPostgresDialect.INSTANCE, operations);

		EntityWithSequence processed = (EntityWithSequence) subject.onBeforeSave(new EntityWithSequence(),
				MutableAggregateChange.forSave(new EntityWithSequence()));

		assertThat(processed.getId()).isEqualTo(generatedId);
	}

	@Test // GH-2003
	void appliesIntegerConversion() {

		long generatedId = 112L;
		when(operations.queryForObject(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
				.thenReturn(generatedId);

		IdGeneratingEntityCallback subject = new IdGeneratingEntityCallback(relationalMappingContext,
				JdbcPostgresDialect.INSTANCE, operations);

		EntityWithIntSequence processed = (EntityWithIntSequence) subject.onBeforeSave(new EntityWithIntSequence(),
				MutableAggregateChange.forSave(new EntityWithIntSequence()));

		assertThat(processed.id).isEqualTo(112);
	}

	@Test // GH-2003
	void assignsUuidValues() {

		UUID generatedId = UUID.randomUUID();
		when(operations.queryForObject(anyString(), any(SqlParameterSource.class), any(RowMapper.class)))
				.thenReturn(generatedId);

		IdGeneratingEntityCallback subject = new IdGeneratingEntityCallback(relationalMappingContext,
				JdbcPostgresDialect.INSTANCE, operations);

		EntityWithUuidSequence processed = (EntityWithUuidSequence) subject.onBeforeSave(new EntityWithUuidSequence(),
				MutableAggregateChange.forSave(new EntityWithUuidSequence()));

		assertThat(processed.id).isEqualTo(generatedId);
	}

	@Table
	static class NoSequenceEntity {

		@Id private Long id;
		private Long name;
	}

	@Table
	static class EntityWithSequence {

		@Id
		@Sequence(value = "id_seq", schema = "public") private Long id;

		private Long name;

		public Long getId() {
			return id;
		}
	}

	@Table
	static class EntityWithIntSequence {

		@Id
		@Sequence(value = "id_seq") private int id;

	}

	@Table
	static class EntityWithUuidSequence {

		@Id
		@Sequence(value = "id_seq") private UUID id;

	}
}
