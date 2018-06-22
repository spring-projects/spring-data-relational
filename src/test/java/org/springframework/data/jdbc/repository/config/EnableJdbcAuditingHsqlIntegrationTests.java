/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.assertj.core.api.SoftAssertions;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests the {@link EnableJdbcAuditing} annotation.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 */
@ActiveProfiles("hsql")
public class EnableJdbcAuditingHsqlIntegrationTests {

	SoftAssertions softly = new SoftAssertions();

	@Test
	public void auditForAnnotatedEntity() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				TestConfiguration.class, //
				AuditingConfiguration.class) //
						.accept(repository -> {

							AuditingConfiguration.currentAuditor = "user01";
							LocalDateTime now = LocalDateTime.now();

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							softly.assertThat(entity.id).isNotNull();
							softly.assertThat(entity.getCreatedBy()).isEqualTo("user01");
							softly.assertThat(entity.getCreatedDate()).isAfter(now);
							softly.assertThat(entity.getLastModifiedBy()).isEqualTo("user01");
							softly.assertThat(entity.getLastModifiedDate()).isAfterOrEqualTo(entity.getCreatedDate());
							softly.assertThat(entity.getLastModifiedDate()).isAfter(now);
							softly.assertThat(repository.findById(entity.id).get()).isEqualTo(entity);

							LocalDateTime beforeCreatedDate = entity.getCreatedDate();
							LocalDateTime beforeLastModifiedDate = entity.getLastModifiedDate();

							softly.assertAll();

							sleepMillis(10);

							AuditingConfiguration.currentAuditor = "user02";

							entity = repository.save(entity);

							softly.assertThat(entity.getCreatedBy()).isEqualTo("user01");
							softly.assertThat(entity.getCreatedDate()).isEqualTo(beforeCreatedDate);
							softly.assertThat(entity.getLastModifiedBy()).isEqualTo("user02");
							softly.assertThat(entity.getLastModifiedDate()).isAfter(beforeLastModifiedDate);
							softly.assertThat(repository.findById(entity.id).get()).isEqualTo(entity);
						});
	}

	@Test
	public void noAnnotatedEntity() {

		configureRepositoryWith( //
				DummyEntityRepository.class, //
				TestConfiguration.class, //
				AuditingConfiguration.class) //
						.accept(repository -> {

							DummyEntity entity = repository.save(new DummyEntity());

							softly.assertThat(entity.id).isNotNull();
							softly.assertThat(repository.findById(entity.id).get()).isEqualTo(entity);

							softly.assertAll();

							entity = repository.save(entity);

							assertThat(repository.findById(entity.id).get()).isEqualTo(entity);
						});
	}

	@Test
	public void customDateTimeProvider() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				TestConfiguration.class, //
				CustomizeAuditorAwareAndDateTimeProvider.class) //
						.accept(repository -> {

							LocalDateTime currentDateTime = LocalDate.of(2018, 4, 14).atStartOfDay();
							CustomizeAuditorAwareAndDateTimeProvider.currentDateTime = currentDateTime;

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							softly.assertThat(entity.id).isNotNull();
							softly.assertThat(entity.getCreatedBy()).isEqualTo("custom user");
							softly.assertThat(entity.getCreatedDate()).isEqualTo(currentDateTime);
							softly.assertThat(entity.getLastModifiedBy()).isNull();
							softly.assertThat(entity.getLastModifiedDate()).isNull();
						});
	}

	@Test
	public void customAuditorAware() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				TestConfiguration.class, //
				CustomizeAuditorAware.class) //
						.accept(repository -> {

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							softly.assertThat(entity.id).isNotNull();
							softly.assertThat(entity.getCreatedBy()).isEqualTo("user");
							softly.assertThat(entity.getCreatedDate()).isNull();
							softly.assertThat(entity.getLastModifiedBy()).isEqualTo("user");
							softly.assertThat(entity.getLastModifiedDate()).isNull();
						});
	}

	/**
	 * Usage looks like this:
	 * <p>
	 * {@code configure(MyRepository.class, MyConfiguration) .accept(repository -> { // perform tests on repository here
	 * }); }
	 *
	 * @param repositoryType the type of repository you want to perform tests on.
	 * @param configurationClasses the classes containing the configuration for the
	 *          {@link org.springframework.context.ApplicationContext}.
	 * @param <T> type of the entity managed by the repository.
	 * @param <R> type of the repository.
	 * @return a Consumer for repositories of type {@code R}.
	 */
	private <T, R extends CrudRepository<T, Long>> Consumer<Consumer<R>> configureRepositoryWith(Class<R> repositoryType,
			Class... configurationClasses) {

		return (Consumer<R> test) -> {

			try (ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(configurationClasses)) {

				test.accept(context.getBean(repositoryType));

				softly.assertAll();
			}
		};
	}

	private void sleepMillis(int timeout) {

		try {
			TimeUnit.MILLISECONDS.sleep(timeout);
		} catch (InterruptedException e) {

			throw new RuntimeException("Failed to sleep", e);
		}
	}

	interface AuditingAnnotatedDummyEntityRepository extends CrudRepository<AuditingAnnotatedDummyEntity, Long> {}

	@Data
	static class AuditingAnnotatedDummyEntity {

		@Id long id;
		@CreatedBy String createdBy;
		@CreatedDate LocalDateTime createdDate;
		@LastModifiedBy String lastModifiedBy;
		@LastModifiedDate LocalDateTime lastModifiedDate;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Data
	static class DummyEntity {

		@Id private Long id;
		// not actually used, exists just to avoid empty value list during insert.
		String name;
	}

	@ComponentScan("org.springframework.data.jdbc.testing")
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class TestConfiguration {

		@Bean
		Class<?> testClass() {
			return EnableJdbcAuditingHsqlIntegrationTests.class;
		}

		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				public String getTableName(@NotNull Class<?> type) {
					return "DummyEntity";
				}
			};
		}
	}

	@EnableJdbcAuditing
	static class AuditingConfiguration {
		static String currentAuditor;

		@Bean
		AuditorAware<String> auditorAware() {
			return () -> Optional.ofNullable(currentAuditor);
		}
	}

	@EnableJdbcAuditing(auditorAwareRef = "customAuditorAware", dateTimeProviderRef = "customDateTimeProvider",
			modifyOnCreate = false)
	static class CustomizeAuditorAwareAndDateTimeProvider {
		static LocalDateTime currentDateTime;

		@Bean
		@Primary
		AuditorAware<String> auditorAware() {
			return () -> Optional.of("default user");
		}

		@Bean
		AuditorAware<String> customAuditorAware() {
			return () -> Optional.of("custom user");
		}

		@Bean
		DateTimeProvider customDateTimeProvider() {
			return () -> Optional.ofNullable(currentDateTime);
		}
	}

	@EnableJdbcAuditing(setDates = false)
	static class CustomizeAuditorAware {

		@Bean
		AuditorAware<String> auditorAware() {
			return () -> Optional.of("user");
		}
	}
}
