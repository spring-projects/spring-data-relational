/*
 * Copyright 2019-2024 the original author or authors.
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * Very simple use cases for creation and usage of JdbcRepositories with test {@link Embedded} annotation in Entities.
 *
 * @author Bastian Wilhelm
 * @author Jens Schauder
 */
@IntegrationTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JdbcRepositoryEmbeddedWithReferenceIntegrationTests {

	public static final String INSERT_MAIN = "INSERT INTO \"DUMMY_ENTITY2\" (\"ID\", \"TEST\") VALUES";
	@Autowired NamedParameterJdbcTemplate template;
	@Autowired DummyEntityRepository repository;
	@Autowired Dialect dialect;

	@Autowired DataSource dataSource;

	private static DummyEntity createDummyEntity() {

		DummyEntity entity = new DummyEntity();
		entity.setTest("root");

		final Embeddable embeddable = new Embeddable();
		embeddable.setTest("embedded");

		final DummyEntity2 dummyEntity2 = new DummyEntity2();
		dummyEntity2.setTest("entity");

		embeddable.setDummyEntity2(dummyEntity2);

		entity.setEmbeddable(embeddable);

		return entity;
	}

	@Test
	@Order(1)
	void dsOne() {
		insertDataSource();
	}

	@Test
	@Order(2)
	void dsTwo() {
		insertDataSource();
	}

	private void insertDataSource() {
		try (Connection connection = dataSource.getConnection()) {

			PreparedStatement preparedStatement = connection.prepareStatement(INSERT_MAIN + "(?, ?)", Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, 4711);
			preparedStatement.setString(2, "text1");
			preparedStatement.executeUpdate();

			try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
				while (resultSet.next()) {
					System.out.println("rs element");
				}
			}
		} catch (SQLException ex) {
			throw new RuntimeException(ex);
		}
	}


	@Test
	@Order(10)
	@Disabled
	void jdbcTemplateOne() {
		insertJdbcTemplate();
	}

	@Test
	@Order(20)
	@Disabled
	void jdbcTemplateTwo() {
		insertJdbcTemplate();
	}



	private void insertJdbcTemplate() {
		Map<String, ?> params = Map.of("id", 4711, "test", "text1");
		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource(params);

		KeyHolder keyHolder = new GeneratedKeyHolder();
		template.update(INSERT_MAIN + " (:id, :test)", sqlParameterSource,
				keyHolder);
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
		List<DummyEntity> findByTest(String test);
	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		DummyEntityRepository dummyEntityRepository(JdbcRepositoryFactory factory) {
			return factory.getRepository(DummyEntityRepository.class);
		}

	}

	private static class DummyEntity {

		@Column("ID")
		@Id Long id;

		String test;

		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "PREFIX_") Embeddable embeddable;

		@Embedded(onEmpty = OnEmpty.USE_NULL) Embeddable2 embeddable2;

		public Long getId() {
			return this.id;
		}

		public String getTest() {
			return this.test;
		}

		public Embeddable getEmbeddable() {
			return this.embeddable;
		}

		public Embeddable2 getEmbeddable2() {
			return this.embeddable2;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setTest(String test) {
			this.test = test;
		}

		public void setEmbeddable(Embeddable embeddable) {
			this.embeddable = embeddable;
		}

		public void setEmbeddable2(Embeddable2 embeddable2) {
			this.embeddable2 = embeddable2;
		}
	}

	private static class Embeddable {

		@Column("ID") DummyEntity2 dummyEntity2;

		String test;

		public DummyEntity2 getDummyEntity2() {
			return this.dummyEntity2;
		}

		public String getTest() {
			return this.test;
		}

		public void setDummyEntity2(DummyEntity2 dummyEntity2) {
			this.dummyEntity2 = dummyEntity2;
		}

		public void setTest(String test) {
			this.test = test;
		}
	}

	private static class Embeddable2 {

		@Column("ID") DummyEntity2 dummyEntity2;

		public DummyEntity2 getDummyEntity2() {
			return this.dummyEntity2;
		}

		public void setDummyEntity2(DummyEntity2 dummyEntity2) {
			this.dummyEntity2 = dummyEntity2;
		}
	}

	private static class DummyEntity2 {

		@Column("ID")
		@Id Long id;

		String test;

		public Long getId() {
			return this.id;
		}

		public String getTest() {
			return this.test;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setTest(String test) {
			this.test = test;
		}
	}
}
