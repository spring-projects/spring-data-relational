/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.repository;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link ConvertedRepository} that uses {@link Converter}s on entity-level.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
public class ConvertingR2dbcRepositoryIntegrationTests {

	@Autowired private ConvertedRepository repository;
	private JdbcTemplate jdbc;

	@Configuration
	@EnableR2dbcRepositories(
			includeFilters = @ComponentScan.Filter(value = ConvertedRepository.class, type = FilterType.ASSIGNABLE_TYPE),
			considerNestedRepositories = true)
	static class TestConfiguration extends AbstractR2dbcConfiguration {
		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}

		@Override
		protected List<Object> getCustomConverters() {
			return Arrays.asList(ConvertedEntityToRow.INSTANCE, RowToConvertedEntity.INSTANCE);
		}
	}

	@Before
	public void before() {

		this.jdbc = new JdbcTemplate(createDataSource());

		try {
			this.jdbc.execute("DROP TABLE CONVERTED_ENTITY");
		} catch (DataAccessException e) {}

		this.jdbc.execute("CREATE TABLE CONVERTED_ENTITY (id serial PRIMARY KEY, name varchar(255))");
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected ConnectionFactory createConnectionFactory() {
		return H2TestSupport.createConnectionFactory();
	}

	@Test
	public void shouldInsertAndReadItems() {

		ConvertedEntity entity = new ConvertedEntity();
		entity.setName("name");

		repository.save(entity) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("read: prefixed: name");
				}).verifyComplete();
	}

	interface ConvertedRepository extends ReactiveCrudRepository<ConvertedEntity, Integer> {

	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Data
	static class ConvertedEntity {
		@Id Integer id;
		String name;
	}

	@WritingConverter
	enum ConvertedEntityToRow implements Converter<ConvertedEntity, OutboundRow> {

		INSTANCE;

		@Override
		public OutboundRow convert(ConvertedEntity convertedEntity) {

			OutboundRow outboundRow = new OutboundRow();

			if (convertedEntity.getId() != null) {
				outboundRow.put("id", Parameter.from(convertedEntity.getId()));
			}

			outboundRow.put("name", Parameter.from("prefixed: " + convertedEntity.getName()));

			return outboundRow;
		}
	}

	@ReadingConverter
	enum RowToConvertedEntity implements Converter<Row, ConvertedEntity> {

		INSTANCE;

		@Override
		public ConvertedEntity convert(Row source) {

			ConvertedEntity entity = new ConvertedEntity();
			entity.setId(source.get("id", Integer.class));
			entity.setName("read: " + source.get("name", String.class));

			return entity;
		}
	}
}
