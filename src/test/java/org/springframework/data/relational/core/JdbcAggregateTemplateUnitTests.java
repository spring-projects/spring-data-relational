/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.jdbc.core;

import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.conversion.BasicRelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class JdbcAggregateTemplateUnitTests {

	JdbcAggregateOperations template;

	@Mock ApplicationEventPublisher eventPublisher;
	@Mock DataSource dataSource;

	@Before
	public void setUp() {

		RelationalMappingContext mappingContext = new RelationalMappingContext(NamingStrategy.INSTANCE);
		RelationalConverter converter = new BasicRelationalConverter(mappingContext);
		NamedParameterJdbcOperations namedParameterJdbcOperations = new NamedParameterJdbcTemplate(dataSource);
		DataAccessStrategy dataAccessStrategy = new DefaultDataAccessStrategy(new SqlGeneratorSource(mappingContext),
				mappingContext, converter, namedParameterJdbcOperations);
		template = new JdbcAggregateTemplate(eventPublisher, mappingContext, converter, dataAccessStrategy);
	}

	@Test // DATAJDBC-378
	public void findAllByIdMustNotAcceptNullArgumentForType() {
		assertThatThrownBy(() -> template.findAllById(singleton(23L), null)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATAJDBC-378
	public void findAllByIdMustNotAcceptNullArgumentForIds() {

		assertThatThrownBy(() -> template.findAllById(null, SampleEntity.class))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATAJDBC-378
	public void findAllByIdWithEmpthListMustReturnEmptyResult() {
		assertThat(template.findAllById(emptyList(), SampleEntity.class)).isEmpty();
	}

	@Data
	private static class SampleEntity {

		@Column("id1") @Id private Long id;

		private String name;
	}
}
