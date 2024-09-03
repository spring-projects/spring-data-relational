/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.DefaultJdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.DelegatingDataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;

/**
 * Extracts the SQL statement that results from declared queries of a repository and perform assertions on it.
 *
 * @author Jens Schauder
 */
public class DeclaredQueryRepositoryUnitTests {

	private NamedParameterJdbcOperations operations = mock(NamedParameterJdbcOperations.class, RETURNS_DEEP_STUBS);

	@Test // GH-1856
	void plainSql() {

		repository(DummyEntityRepository.class).plainQuery();

		assertThat(query()).isEqualTo("select * from someTable");
	}

	@Test // GH-1856
	void tableNameQuery() {

		repository(DummyEntityRepository.class).tableNameQuery();

		assertThat(query()).isEqualTo("select * from \"DUMMY_ENTITY\"");
	}

	@Test // GH-1856
	void renamedTableNameQuery() {

		repository(RenamedEntityRepository.class).tableNameQuery();

		assertThat(query()).isEqualTo("select * from \"ReNamed\"");
	}

	@Test // GH-1856
	void fullyQualifiedTableNameQuery() {

		repository(RenamedEntityRepository.class).qualifiedTableNameQuery();

		assertThat(query()).isEqualTo("select * from \"someSchema\".\"ReNamed\"");
	}

	private String query() {

		ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
		verify(operations).queryForObject(queryCaptor.capture(), any(SqlParameterSource.class), any(RowMapper.class));
		return queryCaptor.getValue();
	}

	private @NotNull <T extends CrudRepository> T repository(Class<T> repositoryInterface) {

		Dialect dialect = HsqlDbDialect.INSTANCE;

		RelationalMappingContext context = new JdbcMappingContext();

		DelegatingDataAccessStrategy delegatingDataAccessStrategy = new DelegatingDataAccessStrategy();
		JdbcConverter converter = new MappingJdbcConverter(context, delegatingDataAccessStrategy,
				new JdbcCustomConversions(), new DefaultJdbcTypeFactory(operations.getJdbcOperations()));

		DataAccessStrategy dataAccessStrategy = mock(DataAccessStrategy.class);
		ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);

		JdbcRepositoryFactory factory = new JdbcRepositoryFactory(dataAccessStrategy, context, converter, dialect,
				publisher, operations);

		return factory.getRepository(repositoryInterface);
	}

	@Table
	record DummyEntity(@Id Long id, String name) {
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

		@Nullable
		@Query("select * from someTable")
		DummyEntity plainQuery();

		@Nullable
		@Query("select * from #{#tableName}")
		DummyEntity tableNameQuery();
	}

	@Table(name = "ReNamed", schema = "someSchema")
	record RenamedEntity(@Id Long id, String name) {
	}

	interface RenamedEntityRepository extends CrudRepository<RenamedEntity, Long> {

		@Nullable
		@Query("select * from #{#tableName}")
		DummyEntity tableNameQuery();

		@Nullable
		@Query("select * from #{#qualifiedTableName}")
		DummyEntity qualifiedTableNameQuery();
	}
}
