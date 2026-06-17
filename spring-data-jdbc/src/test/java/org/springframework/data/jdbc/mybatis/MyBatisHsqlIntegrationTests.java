/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.jdbc.mybatis;

import static org.assertj.core.api.Assertions.*;

import junit.framework.AssertionFailedError;

import java.util.Collections;
import java.util.Map;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.data.jdbc.core.dialect.JdbcHsqlDbDialect;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.DatabaseType;
import org.springframework.data.jdbc.testing.EnabledOnDatabase;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestClass;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;

/**
 * Tests the integration with Mybatis.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
@IntegrationTest
@EnabledOnDatabase(DatabaseType.HSQL)
public class MyBatisHsqlIntegrationTests {

	@Autowired SqlSessionFactory sqlSessionFactory;
	@Autowired DummyEntityRepository repository;
	@Autowired JdbcAggregateOperations template;
	@Autowired NamedParameterJdbcOperations jdbc;

	@Test // DATAJDBC-123
	public void mybatisSelfTest() {

		SqlSession session = sqlSessionFactory.openSession();

		session.selectList("org.springframework.data.jdbc.mybatis.DummyEntityMapper.findById");
	}

	@Test // DATAJDBC-123
	public void myBatisGetsUsedForInsertAndSelect() {

		DummyEntity entity = new DummyEntity(null, "some name");
		DummyEntity saved = repository.save(entity);

		assertThat(saved.id).isNotNull();

		DummyEntity reloaded = repository.findById(saved.id).orElseThrow(AssertionFailedError::new);

		assertThat(reloaded).isNotNull().extracting(e -> e.id, e -> e.name);
	}

	@Test // GH-2316
	public void deleteOfVersionedAggregateWithCurrentVersionSucceeds() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		template.delete(saved);

		Long remaining = jdbc.queryForObject("SELECT COUNT(*) FROM versioned_entity", Collections.emptyMap(),
				Long.class);
		assertThat(remaining).isZero();
	}

	@Test // GH-2316
	public void deleteOfVersionedAggregateWithStaleVersionThrowsOptimisticLockingFailureException() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		// simulate a concurrent update bumping the version in the database
		jdbc.update("UPDATE versioned_entity SET version = version + 1 WHERE id = :id",
				Map.of("id", saved.id));

		assertThatThrownBy(() -> template.delete(saved))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // GH-2316
	public void deleteOfConcurrentlyDeletedVersionedAggregateThrowsOptimisticLockingFailureException() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		// simulate a concurrent deletion
		jdbc.update("DELETE FROM versioned_entity WHERE id = :id", Map.of("id", saved.id));

		assertThatThrownBy(() -> template.delete(saved))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // GH-2316
	public void updateOfVersionedAggregateWithCurrentVersionSucceeds() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		VersionedEntity updated = template.save(new VersionedEntity(saved.id, saved.version, "second"));

		assertThat(updated.version).isEqualTo(saved.version + 1);
		String name = jdbc.queryForObject("SELECT name FROM versioned_entity WHERE id = :id",
				Map.of("id", saved.id), String.class);
		assertThat(name).isEqualTo("second");
	}

	@Test // GH-2316
	public void updateOfVersionedAggregateWithStaleVersionThrowsOptimisticLockingFailureException() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		// simulate a concurrent update bumping the version in the database
		jdbc.update("UPDATE versioned_entity SET version = version + 1 WHERE id = :id",
				Map.of("id", saved.id));

		assertThatThrownBy(() -> template.save(new VersionedEntity(saved.id, saved.version, "second")))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // GH-2316
	public void updateOfConcurrentlyDeletedVersionedAggregateThrowsOptimisticLockingFailureException() {

		VersionedEntity saved = template.save(new VersionedEntity(null, null, "first"));

		// simulate a concurrent deletion
		jdbc.update("DELETE FROM versioned_entity WHERE id = :id", Map.of("id", saved.id));

		assertThatThrownBy(() -> template.save(new VersionedEntity(saved.id, saved.version, "second")))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	interface DummyEntityRepository extends CrudRepository<DummyEntity, Long> {

	}

	@org.springframework.context.annotation.Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true)
	static class Config {

		@Bean
		TestClass testClass() {
			return TestClass.of(MyBatisHsqlIntegrationTests.class);
		}

		@Bean
		SqlSessionFactoryBean createSessionFactory(EmbeddedDatabase db) {

			Configuration configuration = new Configuration();
			configuration.getTypeAliasRegistry().registerAlias("MyBatisContext", MyBatisContext.class);

			configuration.getTypeAliasRegistry().registerAlias(DummyEntity.class);
			configuration.addMapper(DummyEntityMapper.class);

			configuration.getTypeAliasRegistry().registerAlias(VersionedEntity.class);
			configuration.addMapper(VersionedEntityMapper.class);

			SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
			sqlSessionFactoryBean.setDataSource(db);
			sqlSessionFactoryBean.setConfiguration(configuration);

			return sqlSessionFactoryBean;
		}

		@Bean
		SqlSessionTemplate sqlSessionTemplate(SqlSessionFactory factory) {
			return new SqlSessionTemplate(factory);
		}

		@Bean
		@Primary
		DataAccessStrategy dataAccessStrategy(RelationalMappingContext context, JdbcConverter converter,
				SqlSession sqlSession, EmbeddedDatabase db) {

			return MyBatisDataAccessStrategy.createCombinedAccessStrategy(context, converter,
					new NamedParameterJdbcTemplate(db), sqlSession, JdbcHsqlDbDialect.INSTANCE, QueryMappingConfiguration.EMPTY);
		}
	}
}
