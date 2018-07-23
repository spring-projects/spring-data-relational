/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.*;

import junit.framework.AssertionFailedError;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.conversion.DbAction;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

/**
 * Tests that the event infrastructure of Spring Data JDBC is sufficient to manipulate the {@link DbAction}s to be
 * executed against the database.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 */
@ContextConfiguration
public class JdbcRepositoryManipulateDbActionsIntegrationTests {

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired DummyEntityRepository repository;
	@Autowired LogRepository logRepository;

	@Test // DATAJDBC-120
	public void softDelete() {

		// given a persistent entity
		DummyEntity entity = new DummyEntity(null, "Hello");
		repository.save(entity);
		assertThat(entity.id).isNotNull();

		// when I delete the entity
		repository.delete(entity);

		// it is still in the repository, but marked as deleted
		assertThat(repository.findById(entity.id)) //
				.contains(new DummyEntity( //
						entity.id, //
						entity.name, //
						true) //
		);

	}

	@Test // DATAJDBC-120
	public void softDeleteMany() {

		// given persistent entities
		DummyEntity one = new DummyEntity(null, "One");
		DummyEntity two = new DummyEntity(null, "Two");
		repository.saveAll(asList(one, two));

		assertThat(one.id).isNotNull();

		// when I delete the entities
		repository.deleteAll(asList(one, two));

		// they are still in the repository, but marked as deleted
		assertThat(repository.findById(one.id)) //
				.contains(new DummyEntity( //
						one.id, //
						one.name, //
						true) //
		);

		assertThat(repository.findById(two.id)) //
				.contains(new DummyEntity( //
						two.id, //
						two.name, //
						true) //
		);
	}

	@Test // DATAJDBC-120
	public void loggingOnSave() {

		// given a new entity
		DummyEntity one = new DummyEntity(null, "one");

		repository.save(one);
		assertThat(one.id).isNotNull();

		// they are still in the repository, but marked as deleted
		assertThat(logRepository.findById(Config.lastLogId)) //
				.isNotEmpty() //
				.map(Log::getText) //
				.contains("one saved");
	}

	@Test // DATAJDBC-120
	public void loggingOnSaveMany() {

		// given a new entity
		DummyEntity one = new DummyEntity(null, "one");
		DummyEntity two = new DummyEntity(null, "two");

		repository.saveAll(asList(one, two));
		assertThat(one.id).isNotNull();

		// they are still in the repository, but marked as deleted
		assertThat(logRepository.findById(Config.lastLogId)) //
				.isNotEmpty() //
				.map(Log::getText) //
				.contains("two saved");
	}

	@Data
	private static class DummyEntity {

		@Id Long id;
		String name;
		boolean deleted;

		DummyEntity(Long id, String name) {

			this.id = id;
			this.name = name;
			this.deleted = false;
		}

		@PersistenceConstructor
		DummyEntity(Long id, String name, boolean deleted) {

			this.id = id;
			this.name = name;
			this.deleted = deleted;
		}
	}

	private interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {}

	@Getter
	@Setter
	@RequiredArgsConstructor
	private static class Log {

		@Id Long id;
		DummyEntity entity;
		String text;
	}

	private interface LogRepository extends CrudRepository<Log, Long> {}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		static long lastLogId;

		@Bean
		Class<?> testClass() {
			return JdbcRepositoryManipulateDbActionsIntegrationTests.class;
		}

		@Bean
		ApplicationListener<BeforeDeleteEvent> softDeleteListener() {

			return event -> {

				DummyEntity entity = (DummyEntity) event.getOptionalEntity().orElseThrow(AssertionFailedError::new);
				entity.deleted = true;

				List<DbAction<?>> actions = event.getChange().getActions();
				actions.clear();
				actions.add(new DbAction.UpdateRoot<>(entity));
			};
		}

		@Bean
		ApplicationListener<BeforeSaveEvent> logOnSaveListener() {

			// this would actually be easier to implement with an AfterSaveEvent listener, but we want to test AggregateChange
			// manipulation.
			return event -> {

				DummyEntity entity = (DummyEntity) event.getOptionalEntity().orElseThrow(AssertionFailedError::new);
				lastLogId = new Random().nextLong();
				Log log = new Log();
				log.setId(lastLogId);
				log.entity = entity;
				log.text = entity.name + " saved";

				List<DbAction<?>> actions = event.getChange().getActions();
				actions.add(new DbAction.InsertRoot<>(log));
			};
		}
	}
}
