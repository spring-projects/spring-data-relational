/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.domain.ReactiveAuditorAware;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link EnableR2dbcAuditing}
 *
 * @author Mark Paluch
 */
class AuditingUnitTests {

	@EnableR2dbcAuditing(auditorAwareRef = "myAuditor")
	static class AuditingConfiguration {

		@Bean
		ReactiveAuditorAware<String> myAuditor() {
			return () -> Mono.just("Walter");
		}

		@Bean
		R2dbcMappingContext r2dbcMappingContext() {
			return new R2dbcMappingContext();
		}
	}

	@Test // gh-281
	void enablesAuditingAndSetsPropertiesAccordingly() throws Exception {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(AuditingConfiguration.class);

		R2dbcMappingContext mappingContext = context.getBean(R2dbcMappingContext.class);
		mappingContext.getPersistentEntity(Entity.class);

		ReactiveEntityCallbacks callbacks = ReactiveEntityCallbacks.create(context);

		Entity entity = new Entity();
		entity = callbacks.callback(BeforeConvertCallback.class, entity, SqlIdentifier.unquoted("table")).block();

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isEqualTo(entity.created);
		assertThat(entity.modifiedBy).isEqualTo("Walter");

		Thread.sleep(10);
		entity.id = 1L;

		entity = callbacks.callback(BeforeConvertCallback.class, entity, SqlIdentifier.unquoted("table")).block();

		assertThat(entity.created).isNotNull();
		assertThat(entity.modified).isNotEqualTo(entity.created);
		context.close();
	}

	class Entity {

		@Id
		Long id;
		@CreatedDate
		LocalDateTime created;
		@LastModifiedDate
		LocalDateTime modified;
		@LastModifiedBy
		String modifiedBy;

		public Long getId() {
			return this.id;
		}

		public LocalDateTime getCreated() {
			return this.created;
		}

		public LocalDateTime getModified() {
			return this.modified;
		}

		public String getModifiedBy() {
			return this.modifiedBy;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setCreated(LocalDateTime created) {
			this.created = created;
		}

		public void setModified(LocalDateTime modified) {
			this.modified = modified;
		}

		public void setModifiedBy(String modifiedBy) {
			this.modifiedBy = modifiedBy;
		}
	}
}
