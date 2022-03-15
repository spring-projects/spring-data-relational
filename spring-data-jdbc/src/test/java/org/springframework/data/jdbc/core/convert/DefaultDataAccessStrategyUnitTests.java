/*
 * Copyright 2017-2022 the original author or authors.
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

import lombok.RequiredArgsConstructor;

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
public class DefaultDataAccessStrategyUnitTests {

	public static final long ORIGINAL_ID = 4711L;

	NamedParameterJdbcOperations namedJdbcOperations = mock(NamedParameterJdbcOperations.class);
	JdbcOperations jdbcOperations = mock(JdbcOperations.class);
	RelationalMappingContext context = new JdbcMappingContext();
	SqlParametersFactory sqlParametersFactory = mock(SqlParametersFactory.class);
	InsertStrategyFactory insertStrategyFactory = mock(InsertStrategyFactory.class);

	JdbcConverter converter;
	DefaultDataAccessStrategy accessStrategy;

	@BeforeEach
	public void before() {

		DelegatingDataAccessStrategy relationResolver = new DelegatingDataAccessStrategy();
		Dialect dialect = HsqlDbDialect.INSTANCE;
		converter = new BasicJdbcConverter(context, relationResolver, new JdbcCustomConversions(),
				new DefaultJdbcTypeFactory(jdbcOperations), dialect.getIdentifierProcessing());
		accessStrategy = new DefaultDataAccessStrategy( //
				new SqlGeneratorSource(context, converter, dialect), //
				context, //
				converter, //
				namedJdbcOperations, //
				sqlParametersFactory, //
				insertStrategyFactory);

		relationResolver.setDelegate(accessStrategy);
		
		when(sqlParametersFactory.forInsert(any(), any(), any(), any()))
				.thenReturn(new SqlIdentifierParameterSource(dialect.getIdentifierProcessing()));
		when(insertStrategyFactory.insertStrategy(any(), any())).thenReturn(mock(InsertStrategy.class));
		when(insertStrategyFactory.batchInsertStrategy(any(), any())).thenReturn(mock(BatchInsertStrategy.class));
	}

	@Test // GH-1159
	public void insert() {

		accessStrategy.insert(new DummyEntity(ORIGINAL_ID), DummyEntity.class, Identifier.empty(), IdValueSource.PROVIDED);

		verify(insertStrategyFactory).insertStrategy(IdValueSource.PROVIDED, SqlIdentifier.quoted("ID"));
	}

	@Test // GH-1159
	public void batchInsert() {

		accessStrategy.insert(singletonList(InsertSubject.describedBy(new DummyEntity(ORIGINAL_ID), Identifier.empty())), DummyEntity.class, IdValueSource.PROVIDED);

		verify(insertStrategyFactory).batchInsertStrategy(IdValueSource.PROVIDED, SqlIdentifier.quoted("ID"));
	}

	@Test // GH-1159
	public void insertForEntityWithNoId() {

		accessStrategy.insert(new DummyEntityWithoutIdAnnotation(ORIGINAL_ID), DummyEntityWithoutIdAnnotation.class, Identifier.empty(), IdValueSource.GENERATED);

		verify(insertStrategyFactory).insertStrategy(IdValueSource.GENERATED, null);
	}

	@Test // GH-1159
	public void batchInsertForEntityWithNoId() {

		accessStrategy.insert(singletonList(InsertSubject.describedBy(new DummyEntityWithoutIdAnnotation(ORIGINAL_ID), Identifier.empty())), DummyEntityWithoutIdAnnotation.class, IdValueSource.GENERATED);

		verify(insertStrategyFactory).batchInsertStrategy(IdValueSource.GENERATED, null);
	}

	@RequiredArgsConstructor
	private static class DummyEntity {

		@Id private final Long id;
	}

	@RequiredArgsConstructor
	private static class DummyEntityWithoutIdAnnotation {

		private final Long id;
	}
}
