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
package org.springframework.data.jdbc.core;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import org.assertj.core.api.SoftAssertions;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for {@link JdbcAggregateTemplate}.
 *
 * @author Jens Schauder
 */
@ContextConfiguration
@Transactional
public class AggregateTemplateIntegrationTests {

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();
	@Autowired JdbcAggregateOperations template;

	LegoSet legoSet = createLegoSet();

	@Test // DATAJDBC-112
	public void saveAndLoadAnEntityWithReferencedEntityById() {

		template.save(legoSet);

		assertThat(legoSet.manual.id).describedAs("id of stored manual").isNotNull();

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNotNull();

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.getId()) //
				.isEqualTo(legoSet.getManual().getId()) //
				.isNotNull();
		softly.assertThat(reloadedLegoSet.manual.getContent()).isEqualTo(legoSet.getManual().getContent());

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	public void saveAndLoadManyEntitiesWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAll(LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(legoSet.getId(), legoSet.getManual().getId(), legoSet.getManual().getContent()));
	}

	@Test // DATAJDBC-112
	public void saveAndLoadManyEntitiesByIdWithReferencedEntity() {

		template.save(legoSet);

		Iterable<LegoSet> reloadedLegoSets = template.findAllById(singletonList(legoSet.getId()), LegoSet.class);

		assertThat(reloadedLegoSets).hasSize(1).extracting("id", "manual.id", "manual.content")
				.contains(tuple(legoSet.getId(), legoSet.getManual().getId(), legoSet.getManual().getContent()));
	}

	@Test // DATAJDBC-112
	public void saveAndLoadAnEntityWithReferencedNullEntity() {

		legoSet.setManual(null);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual).isNull();
	}

	@Test // DATAJDBC-112
	public void saveAndDeleteAnEntityWithReferencedEntity() {

		template.save(legoSet);

		template.delete(legoSet, LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(template.findAll(LegoSet.class)).isEmpty();
		softly.assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	public void saveAndDeleteAllWithReferencedEntity() {

		template.save(legoSet);

		template.deleteAll(LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		assertThat(template.findAll(LegoSet.class)).isEmpty();
		assertThat(template.findAll(Manual.class)).isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	public void updateReferencedEntityFromNull() {

		legoSet.setManual(null);
		template.save(legoSet);

		Manual manual = new Manual();
		manual.setId(23L);
		manual.setContent("Some content");
		legoSet.setManual(manual);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("Some content");
	}

	@Test // DATAJDBC-112
	public void updateReferencedEntityToNull() {

		template.save(legoSet);

		legoSet.setManual(null);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual).isNull();
		softly.assertThat(template.findAll(Manual.class)).describedAs("Manuals failed to delete").isEmpty();

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	public void replaceReferencedEntity() {

		template.save(legoSet);

		Manual manual = new Manual();
		manual.setContent("other content");
		legoSet.setManual(manual);

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		SoftAssertions softly = new SoftAssertions();

		softly.assertThat(reloadedLegoSet.manual.content).isEqualTo("other content");
		softly.assertThat(template.findAll(Manual.class)).describedAs("There should be only one manual").hasSize(1);

		softly.assertAll();
	}

	@Test // DATAJDBC-112
	public void changeReferencedEntity() {

		template.save(legoSet);

		legoSet.manual.setContent("new content");

		template.save(legoSet);

		LegoSet reloadedLegoSet = template.findById(legoSet.getId(), LegoSet.class);

		assertThat(reloadedLegoSet.manual.content).isEqualTo("new content");
	}

	private static LegoSet createLegoSet() {

		LegoSet entity = new LegoSet();
		entity.setName("Star Destroyer");

		Manual manual = new Manual();
		manual.setContent("Accelerates to 99% of light speed. Destroys almost everything. See https://what-if.xkcd.com/1/");
		entity.setManual(manual);

		return entity;
	}

	@Data
	static class LegoSet {

		@Id private Long id;

		private String name;

		private Manual manual;

	}

	@Data
	static class Manual {

		@Id private Long id;
		private String content;

	}

	@Configuration
	@Import(TestConfiguration.class)
	static class Config {

		@Bean
		Class<?> testClass() {
			return AggregateTemplateIntegrationTests.class;
		}

		@Bean
		JdbcAggregateOperations operations(ApplicationEventPublisher publisher, RelationalMappingContext context,
				DataAccessStrategy dataAccessStrategy, RelationalConverter converter) {
			return new JdbcAggregateTemplate(publisher, context, converter, dataAccessStrategy);
		}
	}
}
