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
package org.springframework.data.jdbc.repository.aot;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.jdbc.core.dialect.JdbcH2Dialect;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.RowMapperFactory;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for the {@link UserRepository} JSON metadata via {@link JdbcRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig(classes = JdbcRepositoryMetadataIntegrationTests.JdbcRepositoryContributorConfiguration.class)
class JdbcRepositoryMetadataIntegrationTests {

	@Autowired AbstractApplicationContext context;

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = String.class) })
	static class JdbcRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {
		public JdbcRepositoryContributorConfiguration() {
			super(UserRepository.class, JdbcH2Dialect.INSTANCE, JdbcRepositoryContributorConfiguration.class);
		}

		@Bean
		RelationalMappingContext mappingContext() {
			return new RelationalMappingContext();
		}

		@Bean
		RowMapperFactory rowMapperFactory() {
			return mock(RowMapperFactory.class);
		}

		@Bean
		JdbcConverter converter(RelationalMappingContext mappingContext) {
			return new MappingJdbcConverter(mappingContext, new RelationResolver() {
				@Override
				public Iterable<Object> findAllByPath(Identifier identifier,
						PersistentPropertyPath<? extends RelationalPersistentProperty> path) {
					return null;
				}
			});
		}

		@Bean
		JdbcAggregateOperations operations(JdbcConverter converter) {

			DataAccessStrategy strategy = mock(DataAccessStrategy.class);
			when(strategy.getDialect()).thenReturn(JdbcH2Dialect.INSTANCE);
			return new JdbcAggregateTemplate(converter, strategy);
		}

	}

	@Test // GH-2121
	void shouldDocumentBase() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).isObject() //
				.containsEntry("name", UserRepository.class.getName()) //
				.containsEntry("module", "JDBC") //
				.containsEntry("type", "IMPERATIVE");
	}

	@Test // GH-2121
	void shouldDocumentDerivedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByFirstname')].query").isArray().first().isObject()
				.containsEntry("query",
						"SELECT \"MY_USER\".\"ID\" AS \"ID\", \"MY_USER\".\"AGE\" AS \"AGE\", \"MY_USER\".\"FIRSTNAME\" AS \"FIRSTNAME\" FROM \"MY_USER\" WHERE \"MY_USER\".\"FIRSTNAME\" = :firstname");
	}

	@Test // GH-2121
	void shouldDocumentDerivedQueryWithParam() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json)
				.inPath("$.methods[?(@.name == 'findWithParameterNameByFirstnameStartingWithOrFirstnameEndingWith')].query")
				.isArray().first().isObject().containsEntry("query",
						"SELECT \"MY_USER\".\"ID\" AS \"ID\", \"MY_USER\".\"AGE\" AS \"AGE\", \"MY_USER\".\"FIRSTNAME\" AS \"FIRSTNAME\" FROM \"MY_USER\" WHERE \"MY_USER\".\"FIRSTNAME\" LIKE :firstname OR (\"MY_USER\".\"FIRSTNAME\" LIKE :firstname1)");
	}

	@Test // GH-2121
	void shouldDocumentPagedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findPageByAgeGreaterThan')].query").isArray().element(0)
				.isObject()
				.containsEntry("query",
						"SELECT \"MY_USER\".\"ID\" AS \"ID\", \"MY_USER\".\"AGE\" AS \"AGE\", \"MY_USER\".\"FIRSTNAME\" AS \"FIRSTNAME\" FROM \"MY_USER\" WHERE \"MY_USER\".\"AGE\" > :age")
				.containsEntry("count-query", "SELECT COUNT(*) FROM \"MY_USER\" WHERE \"MY_USER\".\"AGE\" > :age");
	}

	@Test // GH-2121
	void shouldDocumentQueryWithExpression() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByFirstnameExpression')].query").isArray().first()
				.isObject().containsEntry("query", "SELECT * FROM MY_USER WHERE firstname = :__$synthetic$__1");
	}

	@Test // GH-2121
	void shouldDocumentAnnotatedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByFirstnameAnnotated')].query").isArray().first().isObject()
				.containsEntry("query", "SELECT * FROM MY_USER WHERE firstname = :name");
	}

	@Test // GH-2121
	void shouldDocumentNamedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByNamedQuery')].query").isArray().first().isObject()
				.containsEntry("name", "User.findByNamedQuery").containsEntry("query", "SELECT * FROM USER WHERE NAME = :name");
	}

	@Test // GH-2121
	void shouldDocumentExplicitlyNamedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByAnnotatedNamedQuery')].query").isArray().first()
				.isObject().containsEntry("name", "User.findBySomeAnnotatedNamedQuery")
				.containsEntry("query", "SELECT ANNOTATED FROM USER WHERE NAME = :name");
	}

	@Test // GH-2121
	void shouldDocumentInterfaceProjection() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findInterfaceByFirstname')].query").isArray().first().isObject()
				.containsEntry("query",
						"SELECT \"MY_USER\".\"FIRSTNAME\" AS \"FIRSTNAME\" FROM \"MY_USER\" WHERE \"MY_USER\".\"FIRSTNAME\" = :firstname");
	}

	@Test // GH-2121
	void shouldDocumentBaseFragment() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'existsById')].fragment").isArray().first().isObject()
				.containsEntry("fragment", "org.springframework.data.jdbc.repository.support.SimpleJdbcRepository");
	}

	private Resource getResource() {

		String location = UserRepository.class.getPackageName().replace('.', '/') + "/"
				+ UserRepository.class.getSimpleName() + ".json";
		return new UrlResource(context.getBeanFactory().getBeanClassLoader().getResource(location));
	}

}
