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

import lombok.Data;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jdbc.mapping.model.NamingStrategy;
import org.springframework.data.repository.CrudRepository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@link EnableJdbcAuditing} annotation.
 *
 * @author Kazuki Shimizu
 */
public class EnableJdbcAuditingHsqlIntegrationTests {

	@Test
	public void auditForAnnotatedEntity() throws InterruptedException {
		try (ConfigurableApplicationContext context =
				 new AnnotationConfigApplicationContext(TestConfiguration.class, AuditingConfiguration.class)) {

			AuditingAnnotatedDummyEntityRepository repository = context.getBean(AuditingAnnotatedDummyEntityRepository.class);

			AuditingConfiguration.currentAuditor = "user01";
			LocalDateTime now = LocalDateTime.now();

			AuditingAnnotatedDummyEntity entity = new AuditingAnnotatedDummyEntity();
			entity.setName("Spring Data");
			repository.save(entity);

			assertThat(entity.id).isNotNull();
			assertThat(entity.getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getCreatedDate()).isAfter(now);
			assertThat(entity.getLastModifiedBy()).isEqualTo("user01");
			assertThat(entity.getLastModifiedDate()).isAfterOrEqualTo(entity.getCreatedDate());
			assertThat(entity.getLastModifiedDate()).isAfter(now);
			assertThat(repository.findById(entity.id).get()).isEqualTo(entity);

			LocalDateTime beforeCreatedDate = entity.getCreatedDate();
			LocalDateTime beforeLastModifiedDate = entity.getLastModifiedDate();

			TimeUnit.MILLISECONDS.sleep(100);
			AuditingConfiguration.currentAuditor = "user02";

			entity.setName("Spring Data JDBC");
			repository.save(entity);

			assertThat(entity.getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getCreatedDate()).isEqualTo(beforeCreatedDate);
			assertThat(entity.getLastModifiedBy()).isEqualTo("user02");
			assertThat(entity.getLastModifiedDate()).isAfter(beforeLastModifiedDate);
			assertThat(repository.findById(entity.id).get()).isEqualTo(entity);
		}
	}

	@Test
	public void noAnnotatedEntity() {
		try (ConfigurableApplicationContext context =
				 new AnnotationConfigApplicationContext(TestConfiguration.class, AuditingConfiguration.class)) {

			DummyEntityRepository repository = context.getBean(DummyEntityRepository.class);

			DummyEntity entity = new DummyEntity();
			entity.setName("Spring Data");
			repository.save(entity);

			assertThat(entity.id).isNotNull();
			assertThat(repository.findById(entity.id).get()).isEqualTo(entity);

			entity.setName("Spring Data JDBC");
			repository.save(entity);

			assertThat(repository.findById(entity.id).get()).isEqualTo(entity);
		}
	}

	@Test
	public void customizeEnableJdbcAuditingAttributes() {
		// Test for 'auditorAwareRef', 'dateTimeProviderRef' and 'modifyOnCreate'
		try (ConfigurableApplicationContext context =
				 new AnnotationConfigApplicationContext(TestConfiguration.class, CustomizeAuditingConfiguration1.class)) {
			AuditingAnnotatedDummyEntityRepository repository = context.getBean(AuditingAnnotatedDummyEntityRepository.class);

			LocalDateTime currentDateTime = LocalDate.of(2018, 4, 14).atStartOfDay();
			CustomizeAuditingConfiguration1.currentDateTime = currentDateTime;

			AuditingAnnotatedDummyEntity entity = new AuditingAnnotatedDummyEntity();
			entity.setName("Spring Data JDBC");
			repository.save(entity);

			assertThat(entity.id).isNotNull();
			assertThat(entity.getCreatedBy()).isEqualTo("custom user");
			assertThat(entity.getCreatedDate()).isEqualTo(currentDateTime);
			assertThat(entity.getLastModifiedBy()).isNull();
			assertThat(entity.getLastModifiedDate()).isNull();
		}
		// Test for 'setDates'
		try (ConfigurableApplicationContext context =
				 new AnnotationConfigApplicationContext(TestConfiguration.class, CustomizeAuditingConfiguration2.class)) {
			AuditingAnnotatedDummyEntityRepository repository = context.getBean(AuditingAnnotatedDummyEntityRepository.class);

			AuditingAnnotatedDummyEntity entity = new AuditingAnnotatedDummyEntity();
			entity.setName("Spring Data JDBC");
			repository.save(entity);

			assertThat(entity.id).isNotNull();
			assertThat(entity.getCreatedBy()).isEqualTo("user");
			assertThat(entity.getCreatedDate()).isNull();
			assertThat(entity.getLastModifiedBy()).isEqualTo("user");
			assertThat(entity.getLastModifiedDate()).isNull();
		}
	}


	interface AuditingAnnotatedDummyEntityRepository extends CrudRepository<AuditingAnnotatedDummyEntity, Long> {
	}

	@Data
	static class AuditingAnnotatedDummyEntity {
		@Id
		private Long id;
		private String name;
		@CreatedBy
		private String createdBy;
		@CreatedDate
		private LocalDateTime createdDate;
		@LastModifiedBy
		private String lastModifiedBy;
		@LastModifiedDate
		private LocalDateTime lastModifiedDate;
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {
	}

	@Data
	static class DummyEntity {
		@Id
		private Long id;
		private String name;
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
				public String getTableName(Class<?> type) {
					return "DummyEntity";
				}
			};
		}

	}

	@EnableJdbcAuditing
	static class AuditingConfiguration {
		private static String currentAuditor;
		@Bean
		AuditorAware<String> auditorAware() {
			return () -> Optional.ofNullable(currentAuditor);
		}
	}

	@EnableJdbcAuditing(auditorAwareRef = "customAuditorAware", dateTimeProviderRef = "customDateTimeProvider", modifyOnCreate = false)
	static class CustomizeAuditingConfiguration1 {
		private static LocalDateTime currentDateTime;
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
	static class CustomizeAuditingConfiguration2 {
		@Bean
		AuditorAware<String> auditorAware() {
			return () -> Optional.of("user");
		}
	}

}
