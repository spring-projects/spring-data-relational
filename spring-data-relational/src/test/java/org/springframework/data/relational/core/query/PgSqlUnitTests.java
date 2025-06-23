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
	SimpleQueryRenderContext renderContext = new SimpleQueryRenderContext(table, BindMarkersFactory.named(":", "p", 32),
			converter, context.getRequiredPersistentEntity(WithEmbedding.class));

	@Test
	void distanceLessThan() {
		// distanceOf("embedding").l2()

		Criteria embedding = PgSql
				.where("embedding", it -> it.distanceTo(Vector.of(1, 2, 3), PgSql.VectorSearchFunctions.Distances::cosine))
				.lessThan(0.8);

		String sql = toSql(embedding);

		assertThat(sql).contains("with_embedding.the_embedding <=> :p0 < :p1");
	}

	@Test
	void fieldIsActiveIsTrue() {

		Criteria booleanJsonFieldIsTrue = PgSql.where("properties", it -> it.field("active").asBoolean()).isTrue();

		String sql = toSql(booleanJsonFieldIsTrue);

		assertThat(sql).contains("(with_embedding.properties -> :p0)::boolean = :p1");
	}

	@Test
	void containsArray() {

		// SqlIdentifier to refer to columns?
		Criteria arrayContains = PgSql.where("tags").json().containsAll("electronics", "gaming");

		String sql = toSql(arrayContains);

		assertThat(sql).contains("with_embedding.tags ?& array[:p0, :p1]");
	}

	@Test
	void shouldRenderSourceFunction() {

		// SqlIdentifier to refer to columns?
		Criteria arrayContains = PgSql.where(PgSql.function("array_ndims", CriteriaSource.ofDotPath("someArray")))
				.greaterThan(2);

		String sql = toSql(arrayContains);

		assertThat(sql).contains("array_ndims(with_embedding.some_array) > :p0");
	}

	@Test
	void arrayContains() {

		Criteria someArrayContains = PgSql.where("someArray").contains(PgSql.arrays().arrayOf("electronics", "gaming"));

		String sql = toSql(someArrayContains);

		assertThat(sql).contains("with_embedding.some_array @> array[:p0, :p1]");
	}

	private String toSql(Criteria criteria) {

		QueryExpression.QueryRenderContext contextualized = criteria.getExpression().contextualize(renderContext);
		Expression expression = criteria.getExpression().render(contextualized);

		return SqlRenderer.toString(Select.builder().select(expression).from(table).where(expression).build());
	}

	static class WithEmbedding {

		@Column("the_embedding") Vector embedding;

		String[] someArray;

	}
}
