/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.jdbc.mybatis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests the integration with Mybatis batch mode.
 *
 * @author Kazuki Shimizu
 */
@ContextConfiguration
@Transactional
public class MyBatisBatchHsqlIntegrationTests {

	@org.springframework.context.annotation.Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		@Bean
		Class<?> testClass() {
			return MyBatisBatchHsqlIntegrationTests.class;
		}

		@Bean
		SqlSessionFactoryBean createSessionFactory(EmbeddedDatabase db) {

			Configuration configuration = new Configuration();
			configuration.setDefaultExecutorType(ExecutorType.BATCH);
			configuration.getTypeAliasRegistry().registerAlias("MyBatisContext", MyBatisContext.class);

			configuration.getTypeAliasRegistry().registerAlias(DummyEntity.class);
			configuration.addMapper(DummyEntityMapper.class);

			SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
			sqlSessionFactoryBean.setDataSource(db);
			sqlSessionFactoryBean.setConfiguration(configuration);

			return sqlSessionFactoryBean;
		}

		@Bean
		SqlSessionTemplate SqlSessionTemplate(SqlSessionFactory factory) {
			return new SqlSessionTemplate(factory);
		}

		@Bean
		MyBatisDataAccessStrategy dataAccessStrategy(SqlSessionTemplate template) {
			return new MyBatisDataAccessStrategy(template);
		}
	}

	@ClassRule public static final SpringClassRule classRule = new SpringClassRule();
	@Rule public SpringMethodRule methodRule = new SpringMethodRule();

	@Autowired DummyEntityRepository repository;

	@Test
	public void batchInsertAndSelect() {

		DummyEntity entity1 = new DummyEntity("some name");
		DummyEntity saved1 = repository.save(entity1);
		
		DummyEntity entity2 = new DummyEntity("some name");
		DummyEntity saved2 = repository.save(entity2);

		assertThat(saved1.id).isEqualTo(0);
		assertThat(saved2.id).isEqualTo(0);
		
		Iterator<DummyEntity> loadedEntities = repository.findAll().iterator();

		DummyEntity loaded1 = loadedEntities.next();
		assertThat(saved1.id).isNotEqualTo(0);
		assertThat(loaded1.id).isEqualTo(saved1.id);
		assertThat(loaded1).isNotNull().extracting(e -> e.id, e -> e.name);
		
		DummyEntity loaded2 = loadedEntities.next();
		assertThat(saved2.id).isNotEqualTo(0);
		assertThat(loaded2.id).isEqualTo(saved2.id);
		assertThat(loaded2).isNotNull().extracting(e -> e.id, e -> e.name);
		
		assertThat(loadedEntities.hasNext()).isFalse();
		
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

}
