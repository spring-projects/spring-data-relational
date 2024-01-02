/*
 * Copyright 2017-2024 the original author or authors.
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

import static java.util.Collections.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;

/**
 * Unit tests for {@link DefaultDataAccessStrategy}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Myeonghyeon Lee
 * @author Myat Min
 * @author Radim Tlusty
 * @author Chirag Tailor
 */
class DefaultDataAccessStrategyUnitTests {

	static final long ORIGINAL_ID = 4711L;

	private final NamedParameterJdbcOperations namedJdbcOperations = mock(NamedParameterJdbcOperations.class);
	private final JdbcOperations jdbcOperations = mock(JdbcOperations.class);
	private final RelationalMappingContext context = new JdbcMappingContext();
	private final SqlParametersFactory sqlParametersFactory = mock(SqlParametersFactory.class);
	private final InsertStrategyFactory insertStrategyFactory = mock(InsertStrategyFactory.class);

	private JdbcConverter converter;
	private DataAccessStrategy accessStrategy;

	@BeforeEach
	void before() {

		DelegatingDataAccessStrategy relationResolver = new DelegatingDataAccessStrategy();
		Dialect dialect = HsqlDbDialect.INSTANCE;
		converter = new MappingJdbcConverter(context, relationResolver, new JdbcCustomConversions(),
				new DefaultJdbcTypeFactory(jdbcOperations));
		accessStrategy = new DataAccessStrategyFactory( //
				new SqlGeneratorSource(context, converter, dialect), //
				converter, //
				namedJdbcOperations, //
				sqlParametersFactory, //
				insertStrategyFactory).create();

		relationResolver.setDelegate(accessStrategy);

		when(sqlParametersFactory.forInsert(any(), any(), any(), any())).thenReturn(new SqlIdentifierParameterSource());
		when(insertStrategyFactory.insertStrategy(any(), any())).thenReturn(mock(InsertStrategy.class));
		when(insertStrategyFactory.batchInsertStrategy(any(), any())).thenReturn(mock(BatchInsertStrategy.class));
	}

	@Test // GH-1159
	void insert() {

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, Identifier.empty(), IdValueSource.PROVIDED);

		verify(insertStrategyFactory).insertStrategy(IdValueSource.PROVIDED, SqlIdentifier.quoted("ID"));
	}

	@Test // GH-1159
	void batchInsert() {

		accessStrategy.insert(singletonList(InsertSubject.describedBy(new DummyEntity(ORIGINAL_ID), Identifier.empty())),
				DummyEntity.class, IdValueSource.PROVIDED);

		verify(insertStrategyFactory).batchInsertStrategy(IdValueSource.PROVIDED, SqlIdentifier.quoted("ID"));
	}

	@Test // GH-1159
	void insertForEntityWithNoId() {

		accessStrategy.insert(new DummyEntityWithoutIdAnnotation(ORIGINAL_ID), DummyEntityWithoutIdAnnotation.class,
				Identifier.empty(), IdValueSource.GENERATED);

		verify(insertStrategyFactory).insertStrategy(IdValueSource.GENERATED, null);
	}

	@Test // GH-1159
	void batchInsertForEntityWithNoId() {

		accessStrategy.insert(
				singletonList(InsertSubject.describedBy(new DummyEntityWithoutIdAnnotation(ORIGINAL_ID), Identifier.empty())),
				DummyEntityWithoutIdAnnotation.class, IdValueSource.GENERATED);

		verify(insertStrategyFactory).batchInsertStrategy(IdValueSource.GENERATED, null);
	}

	private static class DummyEntity {

		@Id private final Long id;

		public DummyEntity(Long id) {
			this.id = id;
		}
	}

	private static class DummyEntityWithoutIdAnnotation {

		private final Long id;

		public DummyEntityWithoutIdAnnotation(Long id) {
			this.id = id;
		}
	}
}
