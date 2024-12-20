/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository.config;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.springframework.data.jdbc.core.convert.QueryMappingConfiguration;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link DefaultQueryMappingConfiguration}.
 *
 * @author Jens Schauder
 * @author Evgeni Dimitrov
 */
public class ConfigurableRowMapperMapUnitTests {

	@Test // DATAJDBC-166
	public void freshInstanceReturnsNull() {

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration();

		assertThat(map.getRowMapper(Object.class)).isNull();
	}

	@Test // DATAJDBC-166
	public void returnsConfiguredInstanceForClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(Object.class, rowMapper);

		assertThat(map.getRowMapper(Object.class)).isEqualTo(rowMapper);
	}

	@Test // DATAJDBC-166
	public void returnsNullForClassNotConfigured() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(Number.class, rowMapper);

		assertThat(map.getRowMapper(Integer.class)).isNull();
		assertThat(map.getRowMapper(String.class)).isNull();
	}

	@Test // DATAJDBC-166
	public void returnsInstanceRegisteredForSubClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(String.class, rowMapper);

		assertThat(map.getRowMapper(Object.class)).isEqualTo(rowMapper);
	}

	@Test // DATAJDBC-166
	public void prefersExactTypeMatchClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerRowMapper(Object.class, mock(RowMapper.class)) //
				.registerRowMapper(Integer.class, rowMapper) //
				.registerRowMapper(Number.class, mock(RowMapper.class));

		assertThat(map.getRowMapper(Integer.class)).isEqualTo(rowMapper);
	}

	@Test // DATAJDBC-166
	public void prefersLatestRegistrationForSuperTypeMatch() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerRowMapper(Integer.class, mock(RowMapper.class)) //
				.registerRowMapper(Number.class, rowMapper);

		assertThat(map.getRowMapper(Object.class)).isEqualTo(rowMapper);
	}

}
