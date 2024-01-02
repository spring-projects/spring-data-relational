/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.relational.core.sqlgeneration;

import static org.springframework.data.relational.core.sqlgeneration.SqlAssert.*;

import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Conditions;
import org.springframework.data.relational.core.sql.Table;

/**
 * Tests for {@link SingleQuerySqlGenerator}.
 *
 * @author Jens Schauder
 */
class SingleQuerySqlGeneratorUnitTests {

	RelationalMappingContext context = new RelationalMappingContext();
	Dialect dialect = createDialect();

	@Nested
	class TrivialAggregateWithoutReferences extends AbstractTestFixture {

		TrivialAggregateWithoutReferences() {
			super(TrivialAggregate.class);
		}

		@Test // GH-1446
		void createSelectForFindAll() {

			String sql = sqlGenerator.findAll(persistentEntity);

			SqlAssert fullSelect = assertThatParsed(sql);
			fullSelect.extractOrderBy().isEqualTo(alias("id") + ", rn");

			SqlAssert baseSelect = fullSelect.hasInlineView();

			baseSelect //
					.hasExactlyColumns( //
							col(rnAlias()).as("rn"), //
							col(rnAlias()), //
							col(alias("id")), //
							col(alias("name")) //
					) //
					.hasInlineViewSelectingFrom("\"trivial_aggregate\"") //
					.hasExactlyColumns( //
							lit(1).as(rnAlias()), //
							lit(1).as(rcAlias()), //
							col("\"id\"").as(alias("id")), //
							col("\"name\"").as(alias("name")) //
					);
		}

		@Test // GH-1446
		void createSelectForFindById() {

			Table table = Table.create(persistentEntity.getQualifiedTableName());
			String sql = sqlGenerator.findAll(persistentEntity, table.column("id").isEqualTo(Conditions.just(":id")));

			SqlAssert baseSelect = assertThatParsed(sql).hasInlineView();

			baseSelect //
					.hasExactlyColumns( //
							col(rnAlias()).as("rn"), //
							col(rnAlias()), //
							col(alias("id")), //
							col(alias("name")) //
					) //
					.hasInlineViewSelectingFrom("\"trivial_aggregate\"") //
					.hasExactlyColumns( //
							lit(1).as(rnAlias()), //
							lit(1).as(rcAlias()), //
							col("\"id\"").as(alias("id")), //
							col("\"name\"").as(alias("name")) //
					) //
					.extractWhereClause().isEqualTo("\"trivial_aggregate\".id = :id");
		}

		@Test // GH-1446
		void createSelectForFindAllById() {

			Table table = Table.create(persistentEntity.getQualifiedTableName());
			String sql = sqlGenerator.findAll(persistentEntity, table.column("id").in(Conditions.just(":ids")));

			SqlAssert baseSelect = assertThatParsed(sql).hasInlineView();

			baseSelect //
					.hasExactlyColumns( //
							col(rnAlias()).as("rn"), //
							col(rnAlias()), //
							col(alias("id")), //
							col(alias("name")) //
					) //
					.hasInlineViewSelectingFrom("\"trivial_aggregate\"") //
					.hasExactlyColumns( //
							lit(1).as(rnAlias()), //
							lit(1).as(rcAlias()), //
							col("\"id\"").as(alias("id")), //
							col("\"name\"").as(alias("name")) //
					) //
					.extractWhereClause().isEqualTo("\"trivial_aggregate\".id IN (:ids)");
		}

	}

	@Nested
	class AggregateWithSingleReference extends AbstractTestFixture {

		private AggregateWithSingleReference() {
			super(SingleReferenceAggregate.class);
		}

		@Test // GH-1446
		void createSelectForFindById() {

			Table table = Table.create(persistentEntity.getQualifiedTableName());
			String sql = sqlGenerator.findAll(persistentEntity, table.column("id").isEqualTo(Conditions.just(":id")));

			String rootRowNumber = rnAlias();
			String rootCount = rcAlias();
			String trivialsRowNumber = rnAlias("trivials");
			String backref = backRefAlias("trivials");
			String keyAlias = keyAlias("trivials");

			SqlAssert baseSelect = assertThatParsed(sql).hasInlineView();

			baseSelect //
					.hasExactlyColumns( //

							col(rootRowNumber), //
							col(alias("id")), //
							col(alias("name")), //
							col(trivialsRowNumber), //
							col(alias("trivials.id")), //
							col(alias("trivials.name")), //
							func("greatest", func("coalesce", col(rootRowNumber), lit(1)),
									func("coalesce", col(trivialsRowNumber), lit(1))), //
							col(backref), //
							col(keyAlias) //
					).extractWhereClause() //
					.isEqualTo("");
			baseSelect.hasInlineViewSelectingFrom("\"single_reference_aggregate\"") //
					.hasExactlyColumns( //
							lit(1).as(rnAlias()), lit(1).as(rootCount), //
							col("\"id\"").as(alias("id")), //
							col("\"name\"").as(alias("name")) //
					) //
					.extractWhereClause().isEqualTo("\"single_reference_aggregate\".id = :id");
			baseSelect.hasInlineViewSelectingFrom("\"trivial_aggregate\"") //
					.hasExactlyColumns( //
							rn(col("\"single_reference_aggregate\"")).as(trivialsRowNumber), //
							count(col("\"single_reference_aggregate\"")).as(rcAlias("trivials")), //
							col("\"id\"").as(alias("trivials.id")), //
							col("\"name\"").as(alias("trivials.name")), //
							col("\"single_reference_aggregate\"").as(backref), //
							col("\"single_reference_aggregate_key\"").as(keyAlias) //
					).extractWhereClause().isEmpty();
			baseSelect.hasJoin().on(alias("id"), backref);
		}

	}

	private AggregatePath path(Class<?> type) {
		return context.getAggregatePath(context.getRequiredPersistentEntity(type));
	}

	private AggregatePath path(Class<?> type, String pathAsString) {
		PersistentPropertyPath<RelationalPersistentProperty> persistentPropertyPath = context
				.getPersistentPropertyPath(pathAsString, type);
		return context.getAggregatePath(persistentPropertyPath);
	}

	private static Dialect createDialect() {

		return PostgresDialect.INSTANCE;
	}

	record TrivialAggregate(@Id Long id, String name) {
	}

	record SingleReferenceAggregate(@Id Long id, String name, List<TrivialAggregate> trivials) {
	}

	private class AbstractTestFixture {
		final Class<?> aggregateRootType;
		final SingleQuerySqlGenerator sqlGenerator;
		final RelationalPersistentEntity<?> persistentEntity;
		final AliasFactory aliases;

		private AbstractTestFixture(Class<?> aggregateRootType) {

			this.aggregateRootType = aggregateRootType;
			this.persistentEntity = context.getRequiredPersistentEntity(aggregateRootType);
			this.sqlGenerator = new SingleQuerySqlGenerator(context, new AliasFactory(), dialect);
			this.aliases = sqlGenerator.getAliasFactory();
		}

		AggregatePath path() {
			return SingleQuerySqlGeneratorUnitTests.this.path(aggregateRootType);
		}

		AggregatePath path(String pathAsString) {
			return SingleQuerySqlGeneratorUnitTests.this.path(aggregateRootType, pathAsString);
		}

		protected String rnAlias() {
			return aliases.getRowNumberAlias(path());
		}

		protected String rnAlias(String path) {
			return aliases.getRowNumberAlias(path(path));
		}

		protected String rcAlias() {
			return aliases.getRowCountAlias(path());
		}

		protected String rcAlias(String path) {
			return aliases.getRowCountAlias(path(path));
		}

		protected String alias(String path) {
			return aliases.getColumnAlias(path(path));
		}

		protected String backRefAlias(String path) {
			return aliases.getBackReferenceAlias(path(path));
		}

		protected String keyAlias(String path) {
			return aliases.getKeyAlias(path(path));
		}
	}
}
