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
package org.springframework.data.relational.core.query;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Vector;
import org.springframework.data.relational.core.binding.BindMarkersFactory;
import org.springframework.data.relational.core.conversion.MappingRelationalConverter;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;

/**
 * Unit tests for {@link PgSql} query methods.
 *
 * @author Mark Paluch
 */
class PgSqlUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	MappingRelationalConverter converter = new MappingRelationalConverter(context);
	Table table = Table.create("with_embedding");
	SimpleBindings bindings = new SimpleBindings();
	SimpleEvaluationContext renderContext = new SimpleEvaluationContext(table,
			BindMarkersFactory.named(":", "p", 32).create(), bindings, converter,
			context.getRequiredPersistentEntity(WithEmbedding.class));

	@Test // GH-1953
	void distanceLessThanTransformFunction() {

		// where(distanceOf("embedding", vector).l2()).lessThan(…)

		Criteria embedding = PgSql
				.where("embedding", it -> it.distanceTo(Vector.of(1, 2, 3), PgSql.VectorSearchOperators.Distances::cosine))
				.lessThan("0.8"); // converter converts to Number

		String sql = toSql(embedding);

		assertThat(embedding).hasToString("embedding <=> '[1.0, 2.0, 3.0]' < 0.8");
		assertThat(sql).contains("with_embedding.the_embedding <=> :p0 < :p1");
		assertThat(bindings.getValues()).containsValue(new BigDecimal("0.8"));
	}

	@Test // GH-1953
	void distanceLessThanDistanceStep() {

		Criteria embedding = PgSql.where(PgSql.vectorSearch().distanceOf("embedding", Vector.of(1, 2, 3)).cosine())
				.lessThan("0.8"); // converter converts to Number

		String sql = toSql(embedding);

		assertThat(embedding).hasToString("embedding <=> '[1.0, 2.0, 3.0]' < 0.8");
		assertThat(sql).contains("with_embedding.the_embedding <=> :p0 < :p1");
		assertThat(bindings.getValues()).containsValue(new BigDecimal("0.8"));
	}

	@Test // GH-1953
	void castFieldToBoolean() {

		Criteria booleanJsonFieldIsTrue = PgSql.where("properties", it -> it.field("active")).asBoolean();

		String sql = toSql(booleanJsonFieldIsTrue);

		assertThat(booleanJsonFieldIsTrue).hasToString("(properties -> 'active')::boolean");
		assertThat(sql).contains("(with_embedding.properties -> :p0)::boolean");
	}

	@Test // GH-1953
	void fieldIsActiveIsTrue() {

		Criteria booleanJsonFieldIsTrue = PgSql.where("properties", it -> it.field("active").asBoolean()).isTrue();

		String sql = toSql(booleanJsonFieldIsTrue);

		assertThat(sql).contains("(with_embedding.properties -> :p0)::boolean = :p1");
	}

	@Test // GH-1953
	void jsonContainsAll() {

		Criteria jsonContains = PgSql.where("tags").json(it -> it.containsAll("electronics", "gaming"));

		String sql = toSql(jsonContains);

		assertThat(sql).contains("with_embedding.tags ?& array[:p0, :p1]");
	}

	@Test // GH-1953
	void toJsonEquals() {

		QueryExpression json = PgSql.json().jsonOf(Map.of("foo", "bar"));
		Criteria arrayContains = PgSql.where(json, it -> it.json().index(1)).is("bar");

		String sql = toSql(arrayContains);

		assertThat(sql).contains(":p0::json -> :p1 = :p2");
	}

	@Test // GH-1953
	void arrayOverlapsColumn() {

		Criteria arrayContains = PgSql.where("tags").arrays().overlaps("country");

		String sql = toSql(arrayContains);

		assertThat(sql).contains("with_embedding.tags && with_embedding.country");
	}

	@Test // GH-1953
	void arrayConcatColumnsAndContains() {

		Criteria arrayContains = PgSql.where("tags", it -> it.array().concatWith("other_tags")).arrays().contains("1", "2",
				"3");

		String sql = toSql(arrayContains);

		assertThat(sql).contains("with_embedding.tags || with_embedding.other_tags @> array[:p0, :p1, :p2]");
	}

	@Test // GH-1953
	void shouldRenderSourceFunction() {

		// SqlIdentifier to refer to columns?
		Criteria arrayContains = PgSql.where(PgSql.function("array_ndims", QueryExpression.column("someArray")))
				.greaterThan(2);

		String sql = toSql(arrayContains);

		assertThat(sql).contains("array_ndims(with_embedding.some_array) > :p0");
	}

	@Test // GH-1953
	void arrayContains() {

		Criteria someArrayContains = PgSql.where("someArray").arrays()
				.contains(PgSql.arrays().arrayOf("electronics", "gaming"));

		String sql = toSql(someArrayContains);

		assertThat(sql).contains("with_embedding.some_array @> array[:p0, :p1]");
	}

	private String toSql(Criteria criteria) {

		Expression expression = criteria.getExpression().evaluate(renderContext);
		return SqlRenderer.toString(Select.builder().select(expression).from(table).where(expression).build());
	}

	static class WithEmbedding {

		@Column("the_embedding") Vector embedding;

		String[] someArray;

	}
}
