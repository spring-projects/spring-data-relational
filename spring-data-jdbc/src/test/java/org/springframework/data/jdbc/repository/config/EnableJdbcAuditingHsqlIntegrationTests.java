/*
 * Copyright 2018-2023 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;

/**
 * Tests the {@link EnableJdbcAuditing} annotation.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 * @author Salim Achouche
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
public class EnableJdbcAuditingHsqlIntegrationTests {

	@Test // DATAJDBC-204
	public void auditForAnnotatedEntity() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				Config.class, //
				AuditingConfiguration.class) //
						.accept(repository -> {

							AuditingConfiguration.currentAuditor = "user01";
							LocalDateTime now = LocalDateTime.now();

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							assertThat(entity.id).as("id not null").isNotNull();
							assertThat(entity.getCreatedBy()).as("created by set").isEqualTo("user01");
							assertThat(entity.getCreatedDate()).as("created date set").isAfter(now);
							assertThat(entity.getLastModifiedBy()).as("modified by set").isEqualTo("user01");
							assertThat(entity.getLastModifiedDate()).as("modified date set")
									.isAfterOrEqualTo(entity.getCreatedDate());
							assertThat(entity.getLastModifiedDate()).as("modified date after instance creation").isAfter(now);

							AuditingAnnotatedDummyEntity reloaded = repository.findById(entity.id).get();

							assertThat(reloaded.getCreatedBy()).as("reload created by").isNotNull();
							assertThat(reloaded.getCreatedDate()).as("reload created date").isNotNull();
							assertThat(reloaded.getLastModifiedBy()).as("reload modified by").isNotNull();
							assertThat(reloaded.getLastModifiedDate()).as("reload modified date").isNotNull();

							LocalDateTime beforeCreatedDate = entity.getCreatedDate();
							LocalDateTime beforeLastModifiedDate = entity.getLastModifiedDate();

							sleepMillis(10);

							AuditingConfiguration.currentAuditor = "user02";

							entity = repository.save(entity);

							assertThat(entity.getCreatedBy()).as("created by unchanged").isEqualTo("user01");
							assertThat(entity.getCreatedDate()).as("created date unchanged").isEqualTo(beforeCreatedDate);
							assertThat(entity.getLastModifiedBy()).as("modified by updated").isEqualTo("user02");
							assertThat(entity.getLastModifiedDate()).as("modified date updated")
									.isAfter(beforeLastModifiedDate);

							reloaded = repository.findById(entity.id).get();

							assertThat(reloaded.getCreatedBy()).as("2. reload created by").isNotNull();
							assertThat(reloaded.getCreatedDate()).as("2. reload created date").isNotNull();
							assertThat(reloaded.getLastModifiedBy()).as("2. reload modified by").isNotNull();
							assertThat(reloaded.getLastModifiedDate()).as("2. reload modified date").isNotNull();
						});
	}

	@Test // DATAJDBC-204
	public void noAnnotatedEntity() {

		configureRepositoryWith( //
				DummyEntityRepository.class, //
				Config.class, //
				AuditingConfiguration.class) //
						.accept(repository -> {

							DummyEntity entity = repository.save(new DummyEntity());

							assertThat(entity.id).isNotNull();
							assertThat(repository.findById(entity.id).get()).isEqualTo(entity);

							entity = repository.save(entity);

							assertThat(repository.findById(entity.id)).contains(entity);
						});
	}

	@Test // DATAJDBC-204
	public void customDateTimeProvider() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				Config.class, //
				CustomizeAuditorAwareAndDateTimeProvider.class) //
						.accept(repository -> {

							LocalDateTime currentDateTime = LocalDate.of(2018, 4, 14).atStartOfDay();
							CustomizeAuditorAwareAndDateTimeProvider.currentDateTime = currentDateTime;

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							assertThat(entity.id).isNotNull();
							assertThat(entity.getCreatedBy()).isEqualTo("custom user");
							assertThat(entity.getCreatedDate()).isEqualTo(currentDateTime);
							assertThat(entity.getLastModifiedBy()).isNull();
							assertThat(entity.getLastModifiedDate()).isNull();
						});
	}

	@Test // DATAJDBC-204
	public void customAuditorAware() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				Config.class, //
				CustomizeAuditorAware.class) //
						.accept(repository -> {

							AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

							assertThat(entity.id).isNotNull();
							assertThat(entity.getCreatedBy()).isEqualTo("user");
							assertThat(entity.getCreatedDate()).isNull();
							assertThat(entity.getLastModifiedBy()).isEqualTo("user");
							assertThat(entity.getLastModifiedDate()).isNull();
						});
	}

	@Test // DATAJDBC-390
	public void auditingListenerTriggersBeforeDefaultListener() {

		configureRepositoryWith( //
				AuditingAnnotatedDummyEntityRepository.class, //
				Config.class, //
				AuditingConfiguration.class, //
				OrderAssertingEventListener.class, //
				OrderAssertingCallback.class //
		) //
				.accept(repository -> {

					AuditingAnnotatedDummyEntity entity = repository.save(new AuditingAnnotatedDummyEntity());

					assertThat(entity.id).isNotNull();
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

	static class AuditingAnnotatedDummyEntity {

		@Id long id;
		@CreatedBy String createdBy;
		@CreatedDate LocalDateTime createdDate;
		@LastModifiedBy String lastModifiedBy;
		@LastModifiedDate LocalDateTime lastModifiedDate;

		public long getId() {
			return this.id;
		}

		public String getCreatedBy() {
			return this.createdBy;
		}

		public LocalDateTime getCreatedDate() {
			return this.createdDate;
		}

		public String getLastModifiedBy() {
			return this.lastModifiedBy;
		}

		public LocalDateTime getLastModifiedDate() {
			return this.lastModifiedDate;
		}

		public void setId(long id) {
			this.id = id;
		}

		public void setCreatedBy(String createdBy) {
			this.createdBy = createdBy;
		}

		public void setCreatedDate(LocalDateTime createdDate) {
			this.createdDate = createdDate;
		}

		public void setLastModifiedBy(String lastModifiedBy) {
			this.lastModifiedBy = lastModifiedBy;
		}

		public void setLastModifiedDate(LocalDateTime lastModifiedDate) {
			this.lastModifiedDate = lastModifiedDate;
		}
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	static class DummyEntity {

		@Id private Long id;
		// not actually used, exists just to avoid empty value list during insert.
		String name;

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			DummyEntity that = (DummyEntity) o;
			return Objects.equals(id, that.id) && Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}
	}

	@Configuration
	@EnableJdbcRepositories(considerNestedRepositories = true)
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		TestClass testClass() {
			return TestClass.of(EnableJdbcAuditingHsqlIntegrationTests.class);
		}

		@Bean
		NamingStrategy namingStrategy() {

			return new NamingStrategy() {

				@Override
				public String getTableName(Class<?> type) {
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

	/**
	 * An event listener asserting that it is running after {@link AuditingConfiguration#auditorAware()} was invoked and
	 * set the auditing data.
	 */
	@Component
	static class OrderAssertingEventListener implements ApplicationListener<BeforeSaveEvent> {

		@Override
		public void onApplicationEvent(BeforeSaveEvent event) {

			Object entity = event.getEntity();
			assertThat(entity).isInstanceOf(AuditingAnnotatedDummyEntity.class);
			assertThat(((AuditingAnnotatedDummyEntity) entity).createdDate).isNotNull();
		}
	}

	/**
	 * An event listener asserting that it is running after {@link AuditingConfiguration#auditorAware()} was invoked and
	 * set the auditing data.
	 */
	@Component
	static class OrderAssertingCallback implements BeforeConvertCallback {

		@Override
		public Object onBeforeConvert(Object entity) {

			assertThat(entity).isInstanceOf(AuditingAnnotatedDummyEntity.class);
			assertThat(((AuditingAnnotatedDummyEntity) entity).createdDate).isNotNull();

			return entity;
		}
	}
}
