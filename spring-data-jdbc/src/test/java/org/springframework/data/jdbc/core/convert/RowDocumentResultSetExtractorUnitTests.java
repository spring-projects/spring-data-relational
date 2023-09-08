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

import static org.assertj.core.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.DefaultNamingStrategy;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;

/**
 * Unit tests for the {@link RowDocumentResultSetExtractor}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class RowDocumentResultSetExtractorUnitTests {

	RelationalMappingContext context = new JdbcMappingContext(new DefaultNamingStrategy());

	private final PathToColumnMapping column = new PathToColumnMapping() {
		@Override
		public String column(AggregatePath path) {
			return RowDocumentResultSetExtractorUnitTests.this.column(path);
		}

		@Override
		public String keyColumn(AggregatePath path) {
			return column(path) + "_key";
		}
	};

	RowDocumentResultSetExtractor documentExtractor = new RowDocumentResultSetExtractor(context, column);

	@Test // GH-1446
	void emptyResultSetYieldsEmptyResult() {

		Assertions.setMaxElementsForPrinting(20);

		new ResultSetTester(WithEmbedded.class, context).resultSet(rsc -> {
			rsc.withPaths("id1", "name");
		}).run(resultSet -> {
			assertThatIllegalStateException()
					.isThrownBy(() -> documentExtractor.extractNextDocument(WithEmbedded.class, resultSet));
		});
	}

	@Test // GH-1446
	void singleSimpleEntityGetsExtractedFromSingleRow() throws SQLException {

		testerFor(WithEmbedded.class).resultSet(rsc -> {
			rsc.withPaths("id1", "name") //
					.withRow(1, "Alfred");
		}).run(document -> {

			assertThat(document).containsEntry("id1", 1).containsEntry("name", "Alfred");
		});
	}

	@Test // GH-1446
	void multipleSimpleEntitiesGetExtractedFromMultipleRows() throws SQLException {

		new ResultSetTester(WithEmbedded.class, context).resultSet(rsc -> {
			rsc.withPaths("id1", "name") //
					.withRow(1, "Alfred") //
					.withRow(2, "Bertram");
		}).run(resultSet -> {

			RowDocument document = documentExtractor.extractNextDocument(WithEmbedded.class, resultSet);
			assertThat(document).containsEntry("id1", 1).containsEntry("name", "Alfred");

			RowDocument nextDocument = documentExtractor.extractNextDocument(WithEmbedded.class, resultSet);
			assertThat(nextDocument).containsEntry("id1", 2).containsEntry("name", "Bertram");
		});
	}

	@Nested
	class EmbeddedReference {
		@Test // GH-1446
		void embeddedGetsExtractedFromSingleRow() {

			testerFor(WithEmbedded.class).resultSet(rsc -> {
				rsc.withPaths("id1", "embeddedNullable.dummyName") //
						.withRow(1, "Imani");
			}).run(document -> {

				assertThat(document).containsEntry("id1", 1).containsEntry("dummy_name", "Imani");
			});
		}

		@Test // GH-1446
		void emptyEmbeddedGetsExtractedFromSingleRow() throws SQLException {

			testerFor(WithEmbedded.class).resultSet(rsc -> {
				rsc.withPaths("id1", "embeddedNullable.dummyName") //
						.withRow(1, null);
			}).run(document -> {

				assertThat(document).hasSize(1).containsEntry("id1", 1);
			});
		}
	}

	@Nested
	class ToOneRelationships {
		@Test // GH-1446
		void entityReferenceGetsExtractedFromSingleRow() {

			testerFor(WithOneToOne.class).resultSet(rsc -> {
				rsc.withPaths("id1", "related", "related.dummyName") //
						.withRow(1, 1, "Dummy Alfred");
			}).run(document -> {

				assertThat(document).containsKey("related").containsEntry("related",
						new RowDocument().append("dummy_name", "Dummy Alfred"));
			});
		}

		@Test // GH-1446
		void nullEntityReferenceGetsExtractedFromSingleRow() {

			testerFor(WithOneToOne.class).resultSet(rsc -> {
				rsc.withPaths("id1", "related", "related.dummyName") //
						.withRow(1, null, "Dummy Alfred");
			}).run(document -> {

				assertThat(document).containsKey("related").containsEntry("related",
						new RowDocument().append("dummy_name", "Dummy Alfred"));
			});
		}
	}

	@Nested
	class Sets {

		@Test // GH-1446
		void extractEmptySetReference() {

			testerFor(WithSets.class).resultSet(rsc -> {
				rsc.withPaths("id1", "first", "first.dummyName") //
						.withRow(1, null, null)//
						.withRow(1, null, null) //
						.withRow(1, null, null);
			}).run(document -> {

				assertThat(document).hasSize(1).containsEntry("id1", 1);
			});
		}

		@Test // GH-1446
		void extractSingleSetReference() {

			testerFor(WithSets.class).resultSet(rsc -> {
				rsc.withPath("id1").withKey("first").withPath("first.dummyName") //
						.withRow(1, 1, "Dummy Alfred")//
						.withRow(1, 2, "Dummy Berta") //
						.withRow(1, 3, "Dummy Carl");
			}).run(document -> {

				assertThat(document).containsEntry("id1", 1).containsEntry("first",
						Arrays.asList(RowDocument.of("dummy_name", "Dummy Alfred"), RowDocument.of("dummy_name", "Dummy Berta"),
								RowDocument.of("dummy_name", "Dummy Carl")));
			});
		}

		@Test // GH-1446
		void extractSetReferenceAndSimpleProperty() {

			testerFor(WithSets.class).resultSet(rsc -> {
				rsc.withPaths("id1", "name").withKey("first").withPath("first.dummyName") //
						.withRow(1, "Simplicissimus", 1, "Dummy Alfred")//
						.withRow(1, null, 2, "Dummy Berta") //
						.withRow(1, null, 3, "Dummy Carl");
			}).run(document -> {

				assertThat(document).containsEntry("id1", 1).containsEntry("name", "Simplicissimus").containsEntry("first",
						Arrays.asList(RowDocument.of("dummy_name", "Dummy Alfred"), RowDocument.of("dummy_name", "Dummy Berta"),
								RowDocument.of("dummy_name", "Dummy Carl")));
			});
		}

		@Test // GH-1446
		void extractMultipleSetReference() {

			testerFor(WithSets.class).resultSet(rsc -> {
				rsc.withPaths("id1").withKey("first").withPath("first.dummyName").withKey("second").withPath("second.dummyName") //
						.withRow(1, 1, "Dummy Alfred", 1, "Other Ephraim")//
						.withRow(1, 2, "Dummy Berta", 2, "Other Zeno") //
						.withRow(1, 3, "Dummy Carl", null, null);
			}).run(document -> {

				assertThat(document).hasSize(3)
						.containsEntry("first",
								Arrays.asList(RowDocument.of("dummy_name", "Dummy Alfred"), RowDocument.of("dummy_name", "Dummy Berta"),
										RowDocument.of("dummy_name", "Dummy Carl")))
						.containsEntry("second", Arrays.asList(RowDocument.of("dummy_name", "Other Ephraim"),
								RowDocument.of("dummy_name", "Other Zeno")));
			});
		}

		@Nested
		class Lists {

			@Test // GH-1446
			void extractSingleListReference() {

				testerFor(WithList.class).resultSet(rsc -> {
					rsc.withPaths("id").withKey("withoutIds").withPath("withoutIds.name") //
							.withRow(1, 1, "Dummy Alfred")//
							.withRow(1, 2, "Dummy Berta") //
							.withRow(1, 3, "Dummy Carl");
				}).run(document -> {

					assertThat(document).hasSize(2).containsEntry("without_ids",
							Arrays.asList(RowDocument.of("name", "Dummy Alfred"), RowDocument.of("name", "Dummy Berta"),
									RowDocument.of("name", "Dummy Carl")));
				});
			}

			@Test // GH-1446
			void extractSingleUnorderedListReference() {

				testerFor(WithList.class).resultSet(rsc -> {
					rsc.withPaths("id").withKey("withoutIds").withPath("withoutIds.name") //
							.withRow(1, 0, "Dummy Alfred")//
							.withRow(1, 2, "Dummy Carl") //
							.withRow(1, 1, "Dummy Berta");
				}).run(document -> {

					assertThat(document).containsKey("without_ids");
					List<RowDocument> dummy_list = document.getList("without_ids");
					assertThat(dummy_list).hasSize(3).contains(new RowDocument().append("name", "Dummy Alfred"))
							.contains(new RowDocument().append("name", "Dummy Berta"))
							.contains(new RowDocument().append("name", "Dummy Carl"));
				});
			}
		}
	}

	@Nested
	class Maps {

		@Test // GH-1446
		void extractSingleMapReference() {

			testerFor(WithMaps.class).resultSet(rsc -> {
				rsc.withPaths("id1").withKey("first").withPath("first.dummyName") //
						.withRow(1, "alpha", "Dummy Alfred")//
						.withRow(1, "beta", "Dummy Berta") //
						.withRow(1, "gamma", "Dummy Carl");
			}).run(document -> {

				assertThat(document).containsEntry("first", Map.of("alpha", RowDocument.of("dummy_name", "Dummy Alfred"),
						"beta", RowDocument.of("dummy_name", "Dummy Berta"), "gamma", RowDocument.of("dummy_name", "Dummy Carl")));
			});
		}

		@Test // GH-1446
		void extractMultipleCollectionReference() {

			testerFor(WithMapsAndList.class).resultSet(rsc -> {
				rsc.withPaths("id1").withKey("map").withPath("map.dummyName").withKey("list").withPath("list.name") //
						.withRow(1, "alpha", "Dummy Alfred", 1, "Other Ephraim")//
						.withRow(1, "beta", "Dummy Berta", 2, "Other Zeno") //
						.withRow(1, "gamma", "Dummy Carl", null, null);
			}).run(document -> {

				assertThat(document).containsEntry("map", Map.of("alpha", RowDocument.of("dummy_name", "Dummy Alfred"), //
						"beta", RowDocument.of("dummy_name", "Dummy Berta"), //
						"gamma", RowDocument.of("dummy_name", "Dummy Carl"))) //
						.containsEntry("list",
								Arrays.asList(RowDocument.of("name", "Other Ephraim"), RowDocument.of("name", "Other Zeno")));
			});
		}

		@Test // GH-1446
		void extractNestedMapsWithId() {

			testerFor(WithMaps.class).resultSet(rsc -> {
				rsc.withPaths("id1", "name").withKey("intermediate")
						.withPaths("intermediate.iId", "intermediate.intermediateName").withKey("intermediate.dummyMap")
						.withPaths("intermediate.dummyMap.dummyName")
						//
						.withRow(1, "Alfred", "alpha", 23, "Inami", "omega", "Dustin") //
						.withRow(1, null, "alpha", 23, null, "zeta", "Dora") //
						.withRow(1, null, "beta", 24, "Ina", "eta", "Dotty") //
						.withRow(1, null, "gamma", 25, "Ion", null, null);
			}).run(document -> {

				assertThat(document).containsEntry("id1", 1).containsEntry("name", "Alfred");

				Map<String, Object> intermediate = document.getMap("intermediate");
				assertThat(intermediate).containsKeys("alpha", "beta", "gamma");

				RowDocument alpha = (RowDocument) intermediate.get("alpha");
				assertThat(alpha).containsEntry("i_id", 23).containsEntry("intermediate_name", "Inami");
				Map<String, Object> dummyMap = alpha.getMap("dummy_map");
				assertThat(dummyMap).containsEntry("omega", RowDocument.of("dummy_name", "Dustin")).containsEntry("zeta",
						RowDocument.of("dummy_name", "Dora"));

				RowDocument gamma = (RowDocument) intermediate.get("gamma");
				assertThat(gamma).hasSize(2).containsEntry("i_id", 25).containsEntry("intermediate_name", "Ion");
			});
		}
	}

	private String column(AggregatePath path) {
		return path.toDotPath();
	}

	private static class WithEmbedded {

		@Id long id1;
		String name;
		@Embedded.Nullable DummyEntity embeddedNullable;
		@Embedded.Empty DummyEntity embeddedNonNull;
	}

	private static class WithOneToOne {

		@Id long id1;
		String name;
		DummyEntity related;
	}

	private static class Person {

		String name;
	}

	private static class PersonWithId {

		@Id Long id;
		String name;
	}

	private static class WithList {

		@Id long id;

		List<Person> withoutIds;
		List<PersonWithId> withIds;
	}

	private static class WithSets {

		@Id long id1;
		String name;
		Set<DummyEntity> first;
		Set<DummyEntity> second;
	}

	private static class WithMaps {

		@Id long id1;

		String name;

		Map<String, DummyEntity> first;
		Map<String, Intermediate> intermediate;
		Map<String, IntermediateNoId> noId;
	}

	private static class WithMapsAndList {

		@Id long id1;

		Map<String, DummyEntity> map;
		List<Person> list;
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

	/**
	 * Configurer for a {@link ResultSet}.
	 */
	interface ResultSetConfigurer {

		ResultSetConfigurer withColumns(String... columns);

		/**
		 * Add mapped paths.
		 *
		 * @param path
		 * @return
		 */
		ResultSetConfigurer withPath(String path);

		/**
		 * Add mapped paths.
		 *
		 * @param paths
		 * @return
		 */
		default ResultSetConfigurer withPaths(String... paths) {
			for (String path : paths) {
				withPath(path);
			}

			return this;
		}

		/**
		 * Add mapped key paths.
		 *
		 * @param path
		 * @return
		 */
		ResultSetConfigurer withKey(String path);

		ResultSetConfigurer withRow(Object... values);
	}

	DocumentTester testerFor(Class<?> entityType) {
		return new DocumentTester(entityType, context, documentExtractor);
	}

	private static class AbstractTester {

		private final Class<?> entityType;
		private final RelationalMappingContext context;
		ResultSet resultSet;

		AbstractTester(Class<?> entityType, RelationalMappingContext context) {
			this.entityType = entityType;
			this.context = context;
		}

		AbstractTester resultSet(Consumer<ResultSetConfigurer> configuration) {

			List<Object> values = new ArrayList<>();
			List<String> columns = new ArrayList<>();
			ResultSetConfigurer configurer = new ResultSetConfigurer() {
				@Override
				public ResultSetConfigurer withColumns(String... columnNames) {
					columns.addAll(Arrays.asList(columnNames));
					return this;
				}

				public ResultSetConfigurer withPath(String path) {

					PersistentPropertyPath<RelationalPersistentProperty> propertyPath = context.getPersistentPropertyPath(path,
							entityType);

					columns.add(context.getAggregatePath(propertyPath).toDotPath());
					return this;
				}

				public ResultSetConfigurer withKey(String path) {

					PersistentPropertyPath<RelationalPersistentProperty> propertyPath = context.getPersistentPropertyPath(path,
							entityType);

					columns.add(context.getAggregatePath(propertyPath).toDotPath() + "_key");
					return this;
				}

				@Override
				public ResultSetConfigurer withRow(Object... rowValues) {
					values.addAll(Arrays.asList(rowValues));
					return this;
				}
			};

			configuration.accept(configurer);
			this.resultSet = ResultSetTestUtil.mockResultSet(columns, values.toArray());

			return this;
		}
	}

	private static class DocumentTester extends AbstractTester {

		private final Class<?> entityType;
		private final RowDocumentResultSetExtractor extractor;

		DocumentTester(Class<?> entityType, RelationalMappingContext context, RowDocumentResultSetExtractor extractor) {

			super(entityType, context);

			this.entityType = entityType;
			this.extractor = extractor;
		}

		@Override
		DocumentTester resultSet(Consumer<ResultSetConfigurer> configuration) {

			super.resultSet(configuration);
			return this;
		}

		public void run(ThrowingConsumer<RowDocument> action) {

			try {
				action.accept(extractor.extractNextDocument(entityType, resultSet));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class ResultSetTester extends AbstractTester {

		ResultSetTester(Class<?> entityType, RelationalMappingContext context) {
			super(entityType, context);
		}

		@Override
		ResultSetTester resultSet(Consumer<ResultSetConfigurer> configuration) {

			super.resultSet(configuration);

			return this;
		}

		public void run(ThrowingConsumer<ResultSet> action) {
			action.accept(resultSet);
		}
	}

}
