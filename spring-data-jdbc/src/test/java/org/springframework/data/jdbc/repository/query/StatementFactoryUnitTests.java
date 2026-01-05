/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * Unit tests for {@link StatementFactory}.
 *
 * @author Mark Paluch
 */
class StatementFactoryUnitTests {

	JdbcMappingContext mappingContext = new JdbcMappingContext();
	MappingJdbcConverter converter;

	MapSqlParameterSource parameterSource = new MapSqlParameterSource();
	StatementFactory statementFactory;

	@BeforeEach
	void setUp() {

		JdbcCustomConversions conversions = JdbcCustomConversions.of(JdbcH2Dialect.INSTANCE, List.of());
		mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		mappingContext.setForceQuote(false);
		mappingContext.afterPropertiesSet();
		converter = new MappingJdbcConverter(mappingContext, (identifier, path) -> null, conversions,
				JdbcTypeFactory.unsupported());
		statementFactory = new StatementFactory(converter, JdbcH2Dialect.INSTANCE);
	}

	@Test // GH-2162
	void sliceConsiderSort() {

		StatementFactory.SelectionBuilder selection = statementFactory.slice(User.class);
		selection.page(PageRequest.of(0, 1, Sort.by("id")));

		String sql = selection.build(parameterSource);
		assertThat(sql).contains("SELECT user.ID").contains("ORDER BY user.ID");
	}

	@Test // GH-2162
	void selectConsiderSort() {

		StatementFactory.SelectionBuilder selection = statementFactory.select(User.class);
		selection.page(PageRequest.of(0, 1, Sort.by("id")));

		String sql = selection.build(parameterSource);
		assertThat(sql).contains("SELECT user.ID").contains("ORDER BY user.ID");
	}

	@Test // GH-2162
	void countDoesNotConsiderSort() {

		StatementFactory.SelectionBuilder selection = statementFactory.count(User.class);
		selection.page(PageRequest.of(0, 1, Sort.by("id")));

		String sql = selection.build(parameterSource);
		assertThat(sql).isEqualTo("SELECT COUNT(*) FROM user");
	}

	@Test // GH-2162
	void existsDoesNotConsiderSort() {

		StatementFactory.SelectionBuilder selection = statementFactory.exists(User.class);
		selection.page(PageRequest.of(0, 1, Sort.by("id")));

		String sql = selection.build(parameterSource);
		assertThat(sql).isEqualTo("SELECT user.ID FROM user OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY");
	}

	@Test // GH-2162
	void statementFactoryConsidersQualifiedTableName() {

		StatementFactory.SelectionBuilder selection = statementFactory.select(Media.class);

		String sql = selection.build(parameterSource);
		assertThat(sql).contains("SELECT archive.media.").contains("ROM archive.media");
	}

	@Table(schema = "archive", name = "media")
	public record Media(@Id Long id, String objectType, Long objectId) {
	}

	@Table(name = "user")
	public record User(@Id Long id, String objectType, Long objectId) {
	}

}
