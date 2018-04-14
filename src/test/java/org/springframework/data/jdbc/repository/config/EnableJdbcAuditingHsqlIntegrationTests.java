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
import java.util.ArrayList;
import java.util.List;
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
			entity.setDateOfBirth(LocalDate.of(2000, 12, 4));
			AuditingName name = new AuditingName();
			name.setFirst("Spring");
			name.setLast("Data");
			entity.setName(name);
//			{
//				AuditingEmail email = new AuditingEmail();
//				email.setType("mobile");
//				email.setAddress("test@spring.mobile");
//				entity.getEmails().add(email);
//			}
//			{
//				AuditingEmail email = new AuditingEmail();
//				email.setType("pc");
//				email.setAddress("test@spring.pc");
//				entity.getEmails().add(email);
//			}

			repository.save(entity);

			assertThat(entity.getId()).isNotNull();
			assertThat(entity.getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getCreatedDate()).isAfter(now);
			assertThat(entity.getLastModifiedBy()).isEqualTo("user01");
			assertThat(entity.getLastModifiedDate()).isAfterOrEqualTo(entity.getCreatedDate());
			assertThat(entity.getLastModifiedDate()).isAfter(now);
			assertThat(entity.getName().getId()).isNotNull();
			assertThat(entity.getName().getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getName().getCreatedDate()).isAfter(now);
			assertThat(entity.getName().getLastModifiedBy()).isEqualTo("user01");
			assertThat(entity.getName().getLastModifiedDate()).isAfterOrEqualTo(entity.getName().getCreatedDate());
			assertThat(entity.getName().getLastModifiedDate()).isAfter(now);
//			assertThat(entity.getEmails().get(0).getId()).isNotNull();
//			assertThat(entity.getEmails().get(0).getCreatedBy()).isEqualTo("user01");
//			assertThat(entity.getEmails().get(0).getCreatedDate()).isAfter(now);
//			assertThat(entity.getEmails().get(0).getLastModifiedBy()).isEqualTo("user01");
//			assertThat(entity.getEmails().get(0).getLastModifiedDate()).isAfterOrEqualTo(entity.getEmails().get(0).getCreatedDate());
//			assertThat(entity.getEmails().get(0).getLastModifiedDate()).isAfter(now);
//			assertThat(entity.getEmails().get(1).getId()).isNotNull();
//			assertThat(entity.getEmails().get(1).getCreatedBy()).isEqualTo("user01");
//			assertThat(entity.getEmails().get(1).getCreatedDate()).isAfter(now);
//			assertThat(entity.getEmails().get(1).getLastModifiedBy()).isEqualTo("user01");
//			assertThat(entity.getEmails().get(1).getLastModifiedDate()).isAfterOrEqualTo(entity.getEmails().get(0).getCreatedDate());
//			assertThat(entity.getEmails().get(1).getLastModifiedDate()).isAfter(now);
			assertThat(repository.findById(entity.getId()).get()).isEqualTo(entity);

			LocalDateTime beforeCreatedDate = entity.getCreatedDate();
			LocalDateTime beforeLastModifiedDate = entity.getLastModifiedDate();

			TimeUnit.MILLISECONDS.sleep(100);
			AuditingConfiguration.currentAuditor = "user02";

			name.setFirst("Spring");
			name.setLast("Data JDBC");
			repository.save(entity);

			assertThat(entity.getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getCreatedDate()).isEqualTo(beforeCreatedDate);
			assertThat(entity.getLastModifiedBy()).isEqualTo("user02");
			assertThat(entity.getLastModifiedDate()).isAfter(beforeLastModifiedDate);
			assertThat(entity.getName().getCreatedBy()).isEqualTo("user01");
			assertThat(entity.getName().getCreatedDate()).isEqualTo(beforeCreatedDate);
			assertThat(entity.getName().getLastModifiedBy()).isEqualTo("user02");
			assertThat(entity.getName().getLastModifiedDate()).isAfter(beforeLastModifiedDate);
			assertThat(repository.findById(entity.getId()).get()).isEqualTo(entity);
		}
	}

	@Test
	public void noAnnotatedEntity() {
		try (ConfigurableApplicationContext context =
				 new AnnotationConfigApplicationContext(TestConfiguration.class, AuditingConfiguration.class)) {

			DummyEntityRepository repository = context.getBean(DummyEntityRepository.class);

			DummyEntity entity = new DummyEntity();
			entity.setDateOfBirth(LocalDate.of(2000, 12, 4));
			Name name = new Name();
			name.setFirst("Spring");
			name.setLast("Data");
			entity.setName(name);
			{
				Email email = new Email();
				email.setType("mobile");
				email.setAddress("test@spring.mobile");
				entity.getEmails().add(email);
			}
			{
				Email email = new Email();
				email.setType("pc");
				email.setAddress("test@spring.pc");
				entity.getEmails().add(email);
			}

			repository.save(entity);

			assertThat(entity.getId()).isNotNull();
			assertThat(entity.getName().getId()).isNotNull();
			assertThat(entity.getEmails().get(0).getId()).isNotNull();
			assertThat(entity.getEmails().get(1).getId()).isNotNull();
			assertThat(repository.findById(entity.getId()).get()).isEqualTo(entity);

			name.setFirst("Spring");
			name.setLast("Data JDBC");

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
			AuditingName name = new AuditingName();
			name.setFirst("Spring");
			name.setLast("Data JDBC");
			entity.setName(name);

			repository.save(entity);

			assertThat(entity.getId()).isNotNull();
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
			AuditingName name = new AuditingName();
			name.setFirst("Spring");
			name.setLast("Data JDBC");
			entity.setName(name);

			repository.save(entity);

			assertThat(entity.getId()).isNotNull();
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
		private LocalDate dateOfBirth;
		private AuditingName name;
//		private List<AuditingEmail> emails = new ArrayList<>();
		@CreatedBy
		private String createdBy;
		@CreatedDate
		private LocalDateTime createdDate;
		@LastModifiedBy
		private String lastModifiedBy;
		@LastModifiedDate
		private LocalDateTime lastModifiedDate;
	}

	@Data
	static class AuditingName {
		@Id
		private Long id;
		private String first;
		private String last;
		@CreatedBy
		private String createdBy;
		@CreatedDate
		private LocalDateTime createdDate;
		@LastModifiedBy
		private String lastModifiedBy;
		@LastModifiedDate
		private LocalDateTime lastModifiedDate;
	}

	@Data
	static class AuditingEmail {
		@Id
		private Long id;
		private String type;
		private String address;
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
		private LocalDate dateOfBirth;
		private Name name;
		private List<Email> emails = new ArrayList<>();
	}

	@Data
	static class Name {
		@Id
		private Long id;
		private String first;
		private String last;
	}

	@Data
	static class Email {
		@Id
		private Long id;
		private String type;
		private String address;
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
					if (type.getSimpleName().endsWith("DummyEntity")) {
						return "DummyEntity";
					}
					if (type.getSimpleName().endsWith("Name")) {
						return "Name";
					}
					if (type.getSimpleName().endsWith("Email")) {
						return "Email";
					}
					return type.getSimpleName();
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
