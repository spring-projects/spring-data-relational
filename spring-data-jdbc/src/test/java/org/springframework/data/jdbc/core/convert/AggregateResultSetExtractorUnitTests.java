/*
 * Copyright 2023 the original author or authors.
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

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.jdbc.core.convert.AggregateResultSetExtractorUnitTests.ColumnType.*;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;

/**
 * Unit tests for the {@link AggregateResultSetExtractor}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class AggregateResultSetExtractorUnitTests {

	RelationalMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy());
	JdbcConverter converter = new BasicJdbcConverter(context, mock(RelationResolver.class));

	private final PathToColumnMapping column = new PathToColumnMapping() {
		@Override
		public String column(AggregatePath path) {
			return AggregateResultSetExtractorUnitTests.this.column(path);
		}

		@Override
		public String keyColumn(AggregatePath path) {
			return column(path) + "_key";
		}
	};

	AggregateResultSetExtractor<SimpleEntity> extractor = getExtractor(SimpleEntity.class);

	@Test // GH-1446
	void emptyResultSetYieldsEmptyResult() throws SQLException {

		ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList("T0_C0_ID1", "T0_C1_NAME"));
		assertThat(extractor.extractData(resultSet)).isEmpty();
	}

	@Test // GH-1446
	void singleSimpleEntityGetsExtractedFromSingleRow() throws SQLException {

		ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name")), //
				1, "Alfred");
		assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.name)
				.containsExactly(tuple(1L, "Alfred"));

		resultSet.close();

		RowDocument document = extractor.extractNextDocument(resultSet);

		assertThat(document).containsEntry("id1", 1).containsEntry("name", "Alfred");
	}

	@Test // GH-1446
	void multipleSimpleEntitiesGetExtractedFromMultipleRows() {

		ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name")), //
				1, "Alfred", //
				2, "Bertram" //
		);
		assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.name).containsExactly( //
				tuple(1L, "Alfred"), //
				tuple(2L, "Bertram") //
		);
	}

	@Nested
	class Conversions {

		@Test // GH-1446
		void appliesConversionToProperty() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name")), //
					new BigDecimal(1), "Alfred");
			assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.name)
					.containsExactly(tuple(1L, "Alfred"));
		}

		@Test // GH-1446
		void appliesConversionToConstructorValue() {

			AggregateResultSetExtractor<DummyRecord> extractor = getExtractor(DummyRecord.class);

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name")), //
					new BigDecimal(1), "Alfred");
			assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.name)
					.containsExactly(tuple(1L, "Alfred"));
		}

		@Test // GH-1446
		void appliesConversionToKeyValue() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummyList", KEY), column("dummyList.dummyName")), //
					1, new BigDecimal(0), "Dummy Alfred", //
					1, new BigDecimal(1), "Dummy Berta", //
					1, new BigDecimal(2), "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummyList).extracting(d -> d.dummyName) //
					.containsExactly("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}
	}

	@NotNull
	private <T> AggregateResultSetExtractor<T> getExtractor(Class<T> type) {
		return (AggregateResultSetExtractor<T>) new AggregateResultSetExtractor<>(
				(RelationalPersistentEntity<DummyRecord>) context.getPersistentEntity(type), converter, column);
	}

	@Nested
	class EmbeddedReference {
		@Test // GH-1446
		void embeddedGetsExtractedFromSingleRow() throws SQLException {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("embeddedNullable.dummyName")), //
					1, "Imani");

			assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.embeddedNullable.dummyName)
					.containsExactly(tuple(1L, "Imani"));

			resultSet.close();

			RowDocument document = extractor.extractNextDocument(resultSet);
			assertThat(document).containsEntry("id1", 1).containsEntry("dummy_name", "Imani");
		}

		@Test // GH-1446
		void nullEmbeddedGetsExtractedFromSingleRow() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("embeddedNullable.dummyName")), //
					1, null);

			assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.embeddedNullable)
					.containsExactly(tuple(1L, null));
		}

		@Test // GH-1446
		void emptyEmbeddedGetsExtractedFromSingleRow() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("embeddedNonNull.dummyName")), //
					1, null);

			assertThat(extractor.extractData(resultSet)) //
					.extracting(e -> e.id1, e -> e.embeddedNonNull.dummyName) //
					.containsExactly(tuple(1L, null));
		}
	}

	@Nested
	class ToOneRelationships {
		@Test // GH-1446
		void entityReferenceGetsExtractedFromSingleRow() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummy"), column("dummy.dummyName")), //
					1, 1, "Dummy Alfred");

			assertThat(extractor.extractData(resultSet)) //
					.extracting(e -> e.id1, e -> e.dummy.dummyName) //
					.containsExactly(tuple(1L, "Dummy Alfred"));
		}

		@Test // GH-1446
		void nullEntityReferenceGetsExtractedFromSingleRow() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummy"), column("dummy.dummyName")), //
					1, null, "Dummy Alfred");

			assertThat(extractor.extractData(resultSet)).extracting(e -> e.id1, e -> e.dummy)
					.containsExactly(tuple(1L, null));
		}
	}

	@Nested
	class Sets {

		@Test // GH-1446
		void extractEmptySetReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummies"), column("dummies.dummyName")), //
					1, null, null, //
					1, null, null, //
					1, null, null);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummies).isEmpty();
		}

		@Test // GH-1446
		void extractSingleSetReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummies"), column("dummies.dummyName")), //
					1, 1, "Dummy Alfred", //
					1, 1, "Dummy Berta", //
					1, 1, "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}

		@Test // GH-1446
		void extractSetReferenceAndSimpleProperty() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("name"), column("dummies"), column("dummies.dummyName")), //
					1, "Simplicissimus", 1, "Dummy Alfred", //
					1, null, 1, "Dummy Berta", //
					1, null, 1, "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name).containsExactly(tuple(1L, "Simplicissimus"));
			assertThat(result.iterator().next().dummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}

		@Test // GH-1446
		void extractMultipleSetReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), //
					column("dummies"), column("dummies.dummyName"), //
					column("otherDummies"), column("otherDummies.dummyName")), //
					1, 1, "Dummy Alfred", 1, "Other Ephraim", //
					1, 1, "Dummy Berta", 1, "Other Zeno", //
					1, 1, "Dummy Carl", null, null);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Dummy Alfred", "Dummy Berta", "Dummy Carl");
			assertThat(result.iterator().next().otherDummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Other Ephraim", "Other Zeno");
		}

		@Test // GH-1446
		void extractNestedSetsWithId() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name"), //
					column("intermediates"), column("intermediates.iId"), column("intermediates.intermediateName"), //
					column("intermediates.dummies"), column("intermediates.dummies.dummyName")), //
					1, "Alfred", 1, 23, "Inami", 23, "Dustin", //
					1, null, 1, 23, null, 23, "Dora", //
					1, null, 1, 24, "Ina", 24, "Dotty", //
					1, null, 1, 25, "Ion", null, null, //
					2, "Bon Jovi", 2, 26, "Judith", 26, "Ephraim", //
					2, null, 2, 26, null, 26, "Erin", //
					2, null, 2, 27, "Joel", 27, "Erika", //
					2, null, 2, 28, "Justin", null, null //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediates.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediates.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			final Iterator<SimpleEntity> iter = result.iterator();
			SimpleEntity alfred = iter.next();
			assertThat(alfred).extracting("id1", "name").containsExactly(1L, "Alfred");
			assertThat(alfred.intermediates).extracting(d -> d.intermediateName).containsExactlyInAnyOrder("Inami", "Ina",
					"Ion");

			assertThat(alfred.findInIntermediates("Inami").dummies).extracting(d -> d.dummyName)
					.containsExactlyInAnyOrder("Dustin", "Dora");
			assertThat(alfred.findInIntermediates("Ina").dummies).extracting(d -> d.dummyName)
					.containsExactlyInAnyOrder("Dotty");
			assertThat(alfred.findInIntermediates("Ion").dummies).isEmpty();

			SimpleEntity bonJovy = iter.next();
			assertThat(bonJovy).extracting("id1", "name").containsExactly(2L, "Bon Jovi");
			assertThat(bonJovy.intermediates).extracting(d -> d.intermediateName).containsExactlyInAnyOrder("Judith", "Joel",
					"Justin");
			assertThat(bonJovy.findInIntermediates("Judith").dummies).extracting(d -> d.dummyName)
					.containsExactlyInAnyOrder("Ephraim", "Erin");
			assertThat(bonJovy.findInIntermediates("Joel").dummies).extracting(d -> d.dummyName).containsExactly("Erika");
			assertThat(bonJovy.findInIntermediates("Justin").dummyList).isEmpty();

		}
	}

	@Nested
	class Lists {

		@Test // GH-1446
		void extractSingleListReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummyList", KEY), column("dummyList.dummyName")), //
					1, 0, "Dummy Alfred", //
					1, 1, "Dummy Berta", //
					1, 2, "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummyList).extracting(d -> d.dummyName) //
					.containsExactly("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}

		@Test // GH-1446
		void extractSingleUnorderedListReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummyList", KEY), column("dummyList.dummyName")), //
					1, 0, "Dummy Alfred", //
					1, 2, "Dummy Carl", 1, 1, "Dummy Berta" //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummyList).extracting(d -> d.dummyName) //
					.containsExactly("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}

		@Test // GH-1446
		void extractListReferenceAndSimpleProperty() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("name"), column("dummyList", KEY), column("dummyList.dummyName")), //
					1, "Simplicissimus", 0, "Dummy Alfred", //
					1, null, 1, "Dummy Berta", //
					1, null, 2, "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name).containsExactly(tuple(1L, "Simplicissimus"));
			assertThat(result.iterator().next().dummyList).extracting(d -> d.dummyName) //
					.containsExactly("Dummy Alfred", "Dummy Berta", "Dummy Carl");
		}

		@Test // GH-1446
		void extractMultipleCollectionReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), //
					column("dummyList", KEY), column("dummyList.dummyName"), //
					column("otherDummies"), column("otherDummies.dummyName")), //
					1, 0, "Dummy Alfred", 1, "Other Ephraim", //
					1, 1, "Dummy Berta", 1, "Other Zeno", //
					1, 2, "Dummy Carl", null, null);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			assertThat(result.iterator().next().dummyList).extracting(d -> d.dummyName) //
					.containsExactly("Dummy Alfred", "Dummy Berta", "Dummy Carl");
			assertThat(result.iterator().next().otherDummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Other Ephraim", "Other Zeno");
		}

		@Test // GH-1446
		void extractNestedListsWithId() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name"), //
					column("intermediateList", KEY), column("intermediateList.iId"), column("intermediateList.intermediateName"), //
					column("intermediateList.dummyList", KEY), column("intermediateList.dummyList.dummyName")), //
					1, "Alfred", 0, 23, "Inami", 0, "Dustin", //
					1, null, 0, 23, null, 1, "Dora", //
					1, null, 1, 24, "Ina", 0, "Dotty", //
					1, null, 2, 25, "Ion", null, null, //
					2, "Bon Jovi", 0, 26, "Judith", 0, "Ephraim", //
					2, null, 0, 26, null, 1, "Erin", //
					2, null, 1, 27, "Joel", 0, "Erika", //
					2, null, 2, 28, "Justin", null, null //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediateList.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			final Iterator<SimpleEntity> iter = result.iterator();
			SimpleEntity alfred = iter.next();
			assertThat(alfred).extracting("id1", "name").containsExactly(1L, "Alfred");
			assertThat(alfred.intermediateList).extracting(d -> d.intermediateName).containsExactly("Inami", "Ina", "Ion");

			assertThat(alfred.findInIntermediateList("Inami").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Dustin", "Dora");
			assertThat(alfred.findInIntermediateList("Ina").dummyList).extracting(d -> d.dummyName).containsExactly("Dotty");
			assertThat(alfred.findInIntermediateList("Ion").dummyList).isEmpty();

			SimpleEntity bonJovy = iter.next();
			assertThat(bonJovy).extracting("id1", "name").containsExactly(2L, "Bon Jovi");
			assertThat(bonJovy.intermediateList).extracting(d -> d.intermediateName).containsExactly("Judith", "Joel",
					"Justin");
			assertThat(bonJovy.findInIntermediateList("Judith").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Ephraim", "Erin");
			assertThat(bonJovy.findInIntermediateList("Joel").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Erika");
			assertThat(bonJovy.findInIntermediateList("Justin").dummyList).isEmpty();

		}

		@Test // GH-1446
		void extractNestedListsWithOutId() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name"), //
					column("intermediateListNoId", KEY), column("intermediateListNoId.intermediateName"), //
					column("intermediateListNoId.dummyList", KEY), column("intermediateListNoId.dummyList.dummyName")), //
					1, "Alfred", 0, "Inami", 0, "Dustin", //
					1, null, 0, null, 1, "Dora", //
					1, null, 1, "Ina", 0, "Dotty", //
					1, null, 2, "Ion", null, null, //
					2, "Bon Jovi", 0, "Judith", 0, "Ephraim", //
					2, null, 0, null, 1, "Erin", //
					2, null, 1, "Joel", 0, "Erika", //
					2, null, 2, "Justin", null, null //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediateListNoId.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			final Iterator<SimpleEntity> iter = result.iterator();
			SimpleEntity alfred = iter.next();
			assertThat(alfred).extracting("id1", "name").containsExactly(1L, "Alfred");
			assertThat(alfred.intermediateListNoId).extracting(d -> d.intermediateName).containsExactly("Inami", "Ina",
					"Ion");

			assertThat(alfred.findInIntermediateListNoId("Inami").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Dustin", "Dora");
			assertThat(alfred.findInIntermediateListNoId("Ina").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Dotty");
			assertThat(alfred.findInIntermediateListNoId("Ion").dummyList).isEmpty();

			SimpleEntity bonJovy = iter.next();
			assertThat(bonJovy).extracting("id1", "name").containsExactly(2L, "Bon Jovi");
			assertThat(bonJovy.intermediateListNoId).extracting(d -> d.intermediateName).containsExactly("Judith", "Joel",
					"Justin");

			assertThat(bonJovy.findInIntermediateListNoId("Judith").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Ephraim", "Erin");
			assertThat(bonJovy.findInIntermediateListNoId("Joel").dummyList).extracting(d -> d.dummyName)
					.containsExactly("Erika");
			assertThat(bonJovy.findInIntermediateListNoId("Justin").dummyList).isEmpty();

		}

	}

	@Nested
	class Maps {

		@Test // GH-1446
		void extractSingleMapReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("dummyMap", KEY), column("dummyMap.dummyName")), //
					1, "alpha", "Dummy Alfred", //
					1, "beta", "Dummy Berta", //
					1, "gamma", "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			Map<String, DummyEntity> dummyMap = result.iterator().next().dummyMap;
			assertThat(dummyMap).extracting("alpha").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Alfred");
			assertThat(dummyMap).extracting("beta").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Berta");
			assertThat(dummyMap).extracting("gamma").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Carl");
		}

		@Test // GH-1446
		void extractMapReferenceAndSimpleProperty() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(
					asList(column("id1"), column("name"), column("dummyMap", KEY), column("dummyMap.dummyName")), //
					1, "Simplicissimus", "alpha", "Dummy Alfred", //
					1, null, "beta", "Dummy Berta", //
					1, null, "gamma", "Dummy Carl");

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name).containsExactly(tuple(1L, "Simplicissimus"));
			Map<String, DummyEntity> dummyMap = result.iterator().next().dummyMap;
			assertThat(dummyMap).extracting("alpha").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Alfred");
			assertThat(dummyMap).extracting("beta").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Berta");
			assertThat(dummyMap).extracting("gamma").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Carl");
		}

		@Test // GH-1446
		void extractMultipleCollectionReference() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), //
					column("dummyMap", KEY), column("dummyMap.dummyName"), //
					column("otherDummies"), column("otherDummies.dummyName")), //
					1, "alpha", "Dummy Alfred", 1, "Other Ephraim", //
					1, "beta", "Dummy Berta", 1, "Other Zeno", //
					1, "gamma", "Dummy Carl", null, null);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1).containsExactly(1L);
			Map<String, DummyEntity> dummyMap = result.iterator().next().dummyMap;
			assertThat(dummyMap).extracting("alpha").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Alfred");
			assertThat(dummyMap).extracting("beta").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Berta");
			assertThat(dummyMap).extracting("gamma").extracting(d -> ((DummyEntity) d).dummyName).isEqualTo("Dummy Carl");

			assertThat(result.iterator().next().otherDummies).extracting(d -> d.dummyName) //
					.containsExactlyInAnyOrder("Other Ephraim", "Other Zeno");
		}

		@Test // GH-1446
		void extractNestedMapsWithId() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name"), //
					column("intermediateMap", KEY), column("intermediateMap.iId"), column("intermediateMap.intermediateName"), //
					column("intermediateMap.dummyMap", KEY), column("intermediateMap.dummyMap.dummyName")), //
					1, "Alfred", "alpha", 23, "Inami", "omega", "Dustin", //
					1, null, "alpha", 23, null, "zeta", "Dora", //
					1, null, "beta", 24, "Ina", "eta", "Dotty", //
					1, null, "gamma", 25, "Ion", null, null, //
					2, "Bon Jovi", "phi", 26, "Judith", "theta", "Ephraim", //
					2, null, "phi", 26, null, "jota", "Erin", //
					2, null, "chi", 27, "Joel", "sigma", "Erika", //
					2, null, "psi", 28, "Justin", null, null //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediateMap.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			final Iterator<SimpleEntity> iter = result.iterator();
			SimpleEntity alfred = iter.next();
			assertThat(alfred).extracting("id1", "name").containsExactly(1L, "Alfred");

			assertThat(alfred.intermediateMap.get("alpha").dummyMap.get("omega").dummyName).isEqualTo("Dustin");
			assertThat(alfred.intermediateMap.get("alpha").dummyMap.get("zeta").dummyName).isEqualTo("Dora");
			assertThat(alfred.intermediateMap.get("beta").dummyMap.get("eta").dummyName).isEqualTo("Dotty");
			assertThat(alfred.intermediateMap.get("gamma").dummyMap).isEmpty();

			SimpleEntity bonJovy = iter.next();

			assertThat(bonJovy.intermediateMap.get("phi").dummyMap.get("theta").dummyName).isEqualTo("Ephraim");
			assertThat(bonJovy.intermediateMap.get("phi").dummyMap.get("jota").dummyName).isEqualTo("Erin");
			assertThat(bonJovy.intermediateMap.get("chi").dummyMap.get("sigma").dummyName).isEqualTo("Erika");
			assertThat(bonJovy.intermediateMap.get("psi").dummyMap).isEmpty();
		}

		@Test // GH-1446
		void extractNestedMapsWithOutId() {

			ResultSet resultSet = ResultSetTestUtil.mockResultSet(asList(column("id1"), column("name"), //
					column("intermediateMapNoId", KEY), column("intermediateMapNoId.intermediateName"), //
					column("intermediateMapNoId.dummyMap", KEY), column("intermediateMapNoId.dummyMap.dummyName")), //
					1, "Alfred", "alpha", "Inami", "omega", "Dustin", //
					1, null, "alpha", null, "zeta", "Dora", //
					1, null, "beta", "Ina", "eta", "Dotty", //
					1, null, "gamma", "Ion", null, null, //
					2, "Bon Jovi", "phi", "Judith", "theta", "Ephraim", //
					2, null, "phi", null, "jota", "Erin", //
					2, null, "chi", "Joel", "sigma", "Erika", //
					2, null, "psi", "Justin", null, null //
			);

			Iterable<SimpleEntity> result = extractor.extractData(resultSet);

			assertThat(result).extracting(e -> e.id1, e -> e.name, e -> e.intermediateMapNoId.size())
					.containsExactlyInAnyOrder(tuple(1L, "Alfred", 3), tuple(2L, "Bon Jovi", 3));

			final Iterator<SimpleEntity> iter = result.iterator();
			SimpleEntity alfred = iter.next();
			assertThat(alfred).extracting("id1", "name").containsExactly(1L, "Alfred");

			assertThat(alfred.intermediateMapNoId.get("alpha").dummyMap.get("omega").dummyName).isEqualTo("Dustin");
			assertThat(alfred.intermediateMapNoId.get("alpha").dummyMap.get("zeta").dummyName).isEqualTo("Dora");
			assertThat(alfred.intermediateMapNoId.get("beta").dummyMap.get("eta").dummyName).isEqualTo("Dotty");
			assertThat(alfred.intermediateMapNoId.get("gamma").dummyMap).isEmpty();

			SimpleEntity bonJovy = iter.next();

			assertThat(bonJovy.intermediateMapNoId.get("phi").dummyMap.get("theta").dummyName).isEqualTo("Ephraim");
			assertThat(bonJovy.intermediateMapNoId.get("phi").dummyMap.get("jota").dummyName).isEqualTo("Erin");
			assertThat(bonJovy.intermediateMapNoId.get("chi").dummyMap.get("sigma").dummyName).isEqualTo("Erika");
			assertThat(bonJovy.intermediateMapNoId.get("psi").dummyMap).isEmpty();
		}

	}

	private String column(String path) {
		return column(path, NORMAL);
	}

	private String column(String path, ColumnType columnType) {

		PersistentPropertyPath<RelationalPersistentProperty> propertyPath = context.getPersistentPropertyPath(path,
				SimpleEntity.class);

		return column(context.getAggregatePath(propertyPath)) + (columnType == KEY ? "_key" : "");
	}

	private String column(AggregatePath path) {
		return path.toDotPath();
	}

	enum ColumnType {
		NORMAL, KEY
	}

	private static class SimpleEntity {

		@Id long id1;
		String name;
		DummyEntity dummy;
		@Embedded.Nullable DummyEntity embeddedNullable;
		@Embedded.Empty DummyEntity embeddedNonNull;

		Set<Intermediate> intermediates;

		Set<DummyEntity> dummies;
		Set<DummyEntity> otherDummies;

		List<DummyEntity> dummyList;
		List<Intermediate> intermediateList;
		List<IntermediateNoId> intermediateListNoId;

		Map<String, DummyEntity> dummyMap;
		Map<String, Intermediate> intermediateMap;
		Map<String, IntermediateNoId> intermediateMapNoId;

		Intermediate findInIntermediates(String name) {
			for (Intermediate intermediate : intermediates) {
				if (intermediate.intermediateName.equals(name)) {
					return intermediate;
				}
			}
			fail("No intermediate with name " + name + " found in intermediates.");
			return null;
		}

		Intermediate findInIntermediateList(String name) {
			for (Intermediate intermediate : intermediateList) {
				if (intermediate.intermediateName.equals(name)) {
					return intermediate;
				}
			}
			fail("No intermediate with name " + name + " found in intermediateList.");
			return null;
		}

		IntermediateNoId findInIntermediateListNoId(String name) {
			for (IntermediateNoId intermediate : intermediateListNoId) {
				if (intermediate.intermediateName.equals(name)) {
					return intermediate;
				}
			}
			fail("No intermediates with name " + name + " found in intermediateListNoId.");
			return null;
		}
	}

	private static class Intermediate {

		@Id long iId;
		String intermediateName;

		Set<DummyEntity> dummies;
		List<DummyEntity> dummyList;
		Map<String, DummyEntity> dummyMap;
	}

	private static class IntermediateNoId {

		String intermediateName;

		Set<DummyEntity> dummies;
		List<DummyEntity> dummyList;
		Map<String, DummyEntity> dummyMap;
	}

	private static class DummyEntity {
		String dummyName;
		Long longValue;
	}

	private record DummyRecord(Long id1, String name) {
	}
}
