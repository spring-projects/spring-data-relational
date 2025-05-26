/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.jdbc;

import static org.assertj.core.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.JdbcAggregateTemplate;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Benchmarks for Composite Ids in Spring Data JDBC.
 *
 * @author Mark Paluch
 */
@Testable
public class CompositeIdBenchmarks extends BenchmarkSettings {

	@Configuration
	static class BenchmarkConfiguration extends AbstractJdbcConfiguration {

		@Bean
		NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
			return new NamedParameterJdbcTemplate(dataSource);
		}

		@Bean
		DataSource dataSource() {

			return new EmbeddedDatabaseBuilder() //
					.generateUniqueName(true) //
					.setType(EmbeddedDatabaseType.HSQL) //
					.setScriptEncoding("UTF-8") //
					.ignoreFailedDrops(true) //
					.addScript(
							"classpath:/org.springframework.data.jdbc.core/CompositeIdAggregateTemplateHsqlIntegrationTests-hsql.sql") //
					.build();
		}
	}

	@State(Scope.Benchmark)
	public static class BenchmarkState {

		AnnotationConfigApplicationContext context;
		JdbcAggregateTemplate template;
		NamedParameterJdbcTemplate named;
		AtomicLong l = new AtomicLong();
		SimpleEntity alpha;

		@Setup
		public void setup() throws SQLException {

			context = new AnnotationConfigApplicationContext();
			context.register(BenchmarkConfiguration.class);
			context.refresh();
			context.start();

			template = context.getBean(JdbcAggregateTemplate.class);
			named = context.getBean(NamedParameterJdbcTemplate.class);
			DataSource dataSource = context.getBean(DataSource.class);

			Connection connection = dataSource.getConnection();
			ConnectionHolder holder = new ConnectionHolder(connection, true);
			holder.setSynchronizedWithTransaction(true);
			TransactionSynchronizationManager.bindResource(dataSource, holder);

			alpha = template.insert(new SimpleEntity(new WrappedPk(l.incrementAndGet()), "alpha"));
		}

		@TearDown
		public void cleanup() {
			context.close();
		}
	}

	@Benchmark
	public Object namedTemplate(BenchmarkState state) {
		return state.named.query("SELECT * FROM SIMPLE_ENTITY WHERE id = :id", Map.of("id", state.alpha.wrappedPk.id),
				(rs, rowNum) -> 1);
	}

	@Benchmark
	public Object jdbcTemplate(BenchmarkState state) {
		return state.named.getJdbcOperations().query("SELECT * FROM SIMPLE_ENTITY WHERE id = " + state.alpha.wrappedPk.id,
				(rs, rowNum) -> 1);
	}

	@Benchmark
	public Object baselineInsert(BenchmarkState state) {
		return state.template.insert(new BaselineEntity(state.l.incrementAndGet(), "alpha"));
	}

	@Benchmark
	public Object loadBaselineEntity(BenchmarkState state) {
		return state.template.findById(state.alpha.wrappedPk, BaselineEntity.class);
	}

	@Benchmark
	public Object insert(BenchmarkState state) {
		return state.template.insert(new SimpleEntity(new WrappedPk(state.l.incrementAndGet()), "alpha"));
	}

	@Benchmark
	public Object loadSimpleEntity(BenchmarkState state) {
		return state.template.findById(state.alpha.wrappedPk, SimpleEntity.class);
	}

	@Benchmark
	public Object saveAndLoadEntityWithList(BenchmarkState state) {

		WithList entity = state.template.insert(new WithList(new WrappedPk(state.l.incrementAndGet()), "alpha",
				List.of(new Child("Romulus"), new Child("Remus"))));

		assertThat(entity.wrappedPk).isNotNull() //
				.extracting(WrappedPk::id).isNotNull();

		return state.template.findById(entity.wrappedPk, WithList.class);
	}

	@Benchmark
	public Object saveAndLoadSimpleEntityWithEmbeddedPk(BenchmarkState state) {

		SimpleEntityWithEmbeddedPk entity = state.template
				.insert(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(state.l.incrementAndGet(), "x"), "alpha"));

		return state.template.findById(entity.embeddedPk, SimpleEntityWithEmbeddedPk.class);
	}

	@Benchmark
	public void deleteSingleSimpleEntityWithEmbeddedPk(BenchmarkState state) {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) state.template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha")));

		state.template.delete(entities.get(0));
	}

	@Benchmark
	public void deleteMultipleSimpleEntityWithEmbeddedPk(BenchmarkState state) {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) state.template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta")));

		state.template.deleteAll(List.of(entities.get(1), entities.get(0)));
	}

	@Benchmark
	public void updateSingleSimpleEntityWithEmbeddedPk(BenchmarkState state) {

		List<SimpleEntityWithEmbeddedPk> entities = (List<SimpleEntityWithEmbeddedPk>) state.template
				.insertAll(List.of(new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "alpha"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "y"), "beta"),
						new SimpleEntityWithEmbeddedPk(new EmbeddedPk(24L, "y"), "gamma")));

		SimpleEntityWithEmbeddedPk updated = new SimpleEntityWithEmbeddedPk(new EmbeddedPk(23L, "x"), "ALPHA");
		state.template.save(updated);

		state.template.deleteAll(SimpleEntityWithEmbeddedPk.class);
	}

	private record WrappedPk(Long id) {
	}

	@Table("SIMPLE_ENTITY")
	private record BaselineEntity( //
			@Id Long id, //
			String name //
	) {
	}

	private record SimpleEntity( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) WrappedPk wrappedPk, //
			String name //
	) {
	}

	private record Child(String name) {
	}

	private record WithList( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) WrappedPk wrappedPk, //
			String name, List<Child> children) {
	}

	private record EmbeddedPk(Long one, String two) {
	}

	private record SimpleEntityWithEmbeddedPk( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) EmbeddedPk embeddedPk, //
			String name //
	) {
	}

	private record SingleReference( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) EmbeddedPk embeddedPk, //
			String name, //
			Child child) {
	}

	private record WithListAndCompositeId( //
			@Id @Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) EmbeddedPk embeddedPk, //
			String name, //
			List<Child> child) {
	}

}
