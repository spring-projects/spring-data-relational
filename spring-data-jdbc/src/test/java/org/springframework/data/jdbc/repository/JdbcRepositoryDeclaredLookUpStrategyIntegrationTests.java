package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.support.JdbcRepositoryFactory;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.query.QueryLookupStrategy;

/**
 * Test to verify that
 * <code>@EnableJdbcRepositories(queryLookupStrategy = QueryLookupStrategy.Key.USE_DECLARED_QUERY)</code> works as
 * intended. Tests based on logic from
 * {@link org.springframework.data.jdbc.repository.support.JdbcQueryLookupStrategy.DeclaredQueryLookupStrategy}
 *
 * @author Diego Krupitza
 */
class JdbcRepositoryDeclaredLookUpStrategyIntegrationTests
		extends AbstractJdbcRepositoryLookUpStrategyIntegrationTests {

	@Test
	void contextCannotByCreatedDueToFindByNameNotDeclaredQuery() {

		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {

			context.register(JdbcRepositoryDeclaredLookUpStrategyIntegrationTests.Config.class);

			assertThatThrownBy(() -> {
				context.refresh();
				context.getBean(OnesRepository.class);
			}).hasMessageContaining("findByName");
		}
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			queryLookupStrategy = QueryLookupStrategy.Key.USE_DECLARED_QUERY,
			includeFilters = @ComponentScan.Filter(
					value = AbstractJdbcRepositoryLookUpStrategyIntegrationTests.OnesRepository.class,
					type = FilterType.ASSIGNABLE_TYPE))
	static class Config {

		@Autowired JdbcRepositoryFactory factory;

		@Bean
		Class<?> testClass() {
			return AbstractJdbcRepositoryLookUpStrategyIntegrationTests.class;
		}
	}

}
