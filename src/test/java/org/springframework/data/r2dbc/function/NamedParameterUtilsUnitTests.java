/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.function;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import org.springframework.data.r2dbc.dialect.BindMarkersFactory;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.dialect.SqlServerDialect;

/**
 * Unit tests for {@link NamedParameterUtils}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class NamedParameterUtilsUnitTests {

	private final BindMarkersFactory BIND_MARKERS = PostgresDialect.INSTANCE.getBindMarkersFactory();

	@Test // gh-23
	public void shouldParseSql() {

		String sql = "xxx :a yyyy :b :c :a zzzzz";
		ParsedSql psql = NamedParameterUtils.parseSqlStatement(sql);
		assertThat(psql.getParameterNames()).containsExactly("a", "b", "c", "a");
		assertThat(psql.getTotalParameterCount()).isEqualTo(4);
		assertThat(psql.getNamedParameterCount()).isEqualTo(3);

		String sql2 = "xxx &a yyyy ? zzzzz";
		ParsedSql psql2 = NamedParameterUtils.parseSqlStatement(sql2);
		assertThat(psql2.getParameterNames()).containsExactly("a");
		assertThat(psql2.getTotalParameterCount()).isEqualTo(1);
		assertThat(psql2.getNamedParameterCount()).isEqualTo(1);

		String sql3 = "xxx &ä+:ö" + '\t' + ":ü%10 yyyy ? zzzzz";
		ParsedSql psql3 = NamedParameterUtils.parseSqlStatement(sql3);
		assertThat(psql3.getParameterNames()).containsExactly("ä", "ö", "ü");
	}

	@Test // gh-23
	public void substituteNamedParameters() {

		MapBindParameterSource namedParams = new MapBindParameterSource(new HashMap<>());
		namedParams.addValue("a", "a").addValue("b", "b").addValue("c", "c");

		BindableOperation operation = NamedParameterUtils.substituteNamedParameters("xxx :a :b :c",
				PostgresDialect.INSTANCE.getBindMarkersFactory(), namedParams);

		assertThat(operation.toQuery()).isEqualTo("xxx $1 $2 $3");

		BindableOperation operation2 = NamedParameterUtils.substituteNamedParameters("xxx :a :b :c",
				SqlServerDialect.INSTANCE.getBindMarkersFactory(), namedParams);

		assertThat(operation2.toQuery()).isEqualTo("xxx @P0_a @P1_b @P2_c");
	}

	@Test // gh-23
	public void substituteObjectArray() {

		MapBindParameterSource namedParams = new MapBindParameterSource(new HashMap<>());
		namedParams.addValue("a",
				Arrays.asList(new Object[] { "Walter", "Heisenberg" }, new Object[] { "Walt Jr.", "Flynn" }));

		BindableOperation operation = NamedParameterUtils.substituteNamedParameters("xxx :a", BIND_MARKERS, namedParams);

		assertThat(operation.toQuery()).isEqualTo("xxx ($1, $2), ($3, $4)");
	}

	@Test // gh-23
	public void shouldBindObjectArray() {

		MapBindParameterSource namedParams = new MapBindParameterSource(new HashMap<>());
		namedParams.addValue("a",
				Arrays.asList(new Object[] { "Walter", "Heisenberg" }, new Object[] { "Walt Jr.", "Flynn" }));

		Statement<?> mockStatement = mock(Statement.class);

		BindableOperation operation = NamedParameterUtils.substituteNamedParameters("xxx :a", BIND_MARKERS, namedParams);
		operation.bind(mockStatement, "a", namedParams.getValue("a"));

		verify(mockStatement).bind(0, "Walter");
		verify(mockStatement).bind(1, "Heisenberg");
		verify(mockStatement).bind(2, "Walt Jr.");
		verify(mockStatement).bind(3, "Flynn");
	}

	@Test // gh-23
	public void parseSqlContainingComments() {

		String sql1 = "/*+ HINT */ xxx /* comment ? */ :a yyyy :b :c :a zzzzz -- :xx XX\n";

		ParsedSql psql1 = NamedParameterUtils.parseSqlStatement(sql1);
		assertThat(expand(psql1)).isEqualTo("/*+ HINT */ xxx /* comment ? */ $1 yyyy $2 $3 $4 zzzzz -- :xx XX\n");

		MapBindParameterSource paramMap = new MapBindParameterSource(new HashMap<>());
		paramMap.addValue("a", "a");
		paramMap.addValue("b", "b");
		paramMap.addValue("c", "c");

		String sql2 = "/*+ HINT */ xxx /* comment ? */ :a yyyy :b :c :a zzzzz -- :xx XX";
		ParsedSql psql2 = NamedParameterUtils.parseSqlStatement(sql2);
		assertThat(expand(psql2)).isEqualTo("/*+ HINT */ xxx /* comment ? */ $1 yyyy $2 $3 $4 zzzzz -- :xx XX");
	}

	@Test // gh-23
	public void parseSqlStatementWithPostgresCasting() {

		String expectedSql = "select 'first name' from artists where id = $1 and birth_date=$2::timestamp";
		String sql = "select 'first name' from artists where id = :id and birth_date=:birthDate::timestamp";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
		BindableOperation operation = NamedParameterUtils.substituteNamedParameters(parsedSql, BIND_MARKERS,
				new MapBindParameterSource());

		assertThat(operation.toQuery()).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithPostgresContainedOperator() {

		String expectedSql = "select 'first name' from artists where info->'stat'->'albums' = ?? $1 and '[\"1\",\"2\",\"3\"]'::jsonb ?? '4'";
		String sql = "select 'first name' from artists where info->'stat'->'albums' = ?? :album and '[\"1\",\"2\",\"3\"]'::jsonb ?? '4'";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(parsedSql.getTotalParameterCount()).isEqualTo(1);
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithPostgresAnyArrayStringsExistsOperator() {

		String expectedSql = "select '[\"3\", \"11\"]'::jsonb ?| '{1,3,11,12,17}'::text[]";
		String sql = "select '[\"3\", \"11\"]'::jsonb ?| '{1,3,11,12,17}'::text[]";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(parsedSql.getTotalParameterCount()).isEqualTo(0);
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithPostgresAllArrayStringsExistsOperator() {

		String expectedSql = "select '[\"3\", \"11\"]'::jsonb ?& '{1,3,11,12,17}'::text[] AND $1 = 'Back in Black'";
		String sql = "select '[\"3\", \"11\"]'::jsonb ?& '{1,3,11,12,17}'::text[] AND :album = 'Back in Black'";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
		assertThat(parsedSql.getTotalParameterCount()).isEqualTo(1);
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithEscapedColon() {

		String expectedSql = "select '0\\:0' as a, foo from bar where baz < DATE($1 23:59:59) and baz = $2";
		String sql = "select '0\\:0' as a, foo from bar where baz < DATE(:p1 23\\:59\\:59) and baz = :p2";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(parsedSql.getParameterNames()).containsExactly("p1", "p2");
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithBracketDelimitedParameterNames() {

		String expectedSql = "select foo from bar where baz = b$1$2z";
		String sql = "select foo from bar where baz = b:{p1}:{p2}z";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
		assertThat(parsedSql.getParameterNames()).containsExactly("p1", "p2");
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithEmptyBracketsOrBracketsInQuotes() {

		String expectedSql = "select foo from bar where baz = b:{}z";
		String sql = "select foo from bar where baz = b:{}z";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(parsedSql.getParameterNames()).isEmpty();
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);

		String expectedSql2 = "select foo from bar where baz = 'b:{p1}z'";
		String sql2 = "select foo from bar where baz = 'b:{p1}z'";

		ParsedSql parsedSql2 = NamedParameterUtils.parseSqlStatement(sql2);
		assertThat(parsedSql2.getParameterNames()).isEmpty();
		assertThat(expand(parsedSql2)).isEqualTo(expectedSql2);
	}

	@Test // gh-23
	public void parseSqlStatementWithSingleLetterInBrackets() {

		String expectedSql = "select foo from bar where baz = b$1z";
		String sql = "select foo from bar where baz = b:{p}z";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
		assertThat(parsedSql.getParameterNames()).containsExactly("p");
		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithLogicalAnd() {

		String expectedSql = "xxx & yyyy";

		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(expectedSql);

		assertThat(expand(parsedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void substituteNamedParametersWithLogicalAnd() {

		String expectedSql = "xxx & yyyy";

		assertThat(expand(expectedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void variableAssignmentOperator() {

		String expectedSql = "x := 1";

		assertThat(expand(expectedSql)).isEqualTo(expectedSql);
	}

	@Test // gh-23
	public void parseSqlStatementWithQuotedSingleQuote() {

		String sql = "SELECT ':foo'':doo', :xxx FROM DUAL";

		ParsedSql psql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(psql.getTotalParameterCount()).isEqualTo(1);
		assertThat(psql.getParameterNames()).containsExactly("xxx");
	}

	@Test // gh-23
	public void parseSqlStatementWithQuotesAndCommentBefore() {

		String sql = "SELECT /*:doo*/':foo', :xxx FROM DUAL";

		ParsedSql psql = NamedParameterUtils.parseSqlStatement(sql);

		assertThat(psql.getTotalParameterCount()).isEqualTo(1);
		assertThat(psql.getParameterNames()).containsExactly("xxx");
	}

	@Test // gh-23
	public void parseSqlStatementWithQuotesAndCommentAfter() {

		String sql2 = "SELECT ':foo'/*:doo*/, :xxx FROM DUAL";

		ParsedSql psql2 = NamedParameterUtils.parseSqlStatement(sql2);

		assertThat(psql2.getTotalParameterCount()).isEqualTo(1);
		assertThat(psql2.getParameterNames()).containsExactly("xxx");
	}

	private String expand(ParsedSql sql) {
		return NamedParameterUtils.substituteNamedParameters(sql, BIND_MARKERS, new MapBindParameterSource()).toQuery();
	}

	private String expand(String sql) {
		return NamedParameterUtils.substituteNamedParameters(sql, BIND_MARKERS, new MapBindParameterSource()).toQuery();
	}
}
