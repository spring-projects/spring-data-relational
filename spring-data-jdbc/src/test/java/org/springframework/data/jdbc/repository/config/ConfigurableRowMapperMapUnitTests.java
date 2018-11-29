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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.springframework.data.jdbc.repository.QueryMappingConfiguration;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link ConfigurableRowMapperMap}.
 * 
 * @author Jens Schauder
 */
public class ConfigurableRowMapperMapUnitTests {

	@Test
	public void freshInstanceReturnsNull() {

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration();

		assertThat(map.rowMapperFor(Object.class)).isNull();
	}
	
	@Test
	public void freshInstanceReturnsNullResultSetExtractor() {

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration();

		assertThat(map.resultSetExtractorFor(Object.class)).isNull();
	}

	@Test
	public void returnsConfiguredInstanceForClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(Object.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}
	
	@Test
	public void returnsConfiguredInstanceResultSetExtractorForClass() {

		ResultSetExtractor resultSetExtractor = mock(ResultSetExtractor.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerResultSetExtractor(Object.class, resultSetExtractor);

		assertThat(map.resultSetExtractorFor(Object.class)).isEqualTo(resultSetExtractor);
	}

	@Test
	public void returnsNullForClassNotConfigured() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(Number.class, rowMapper);

		assertThat(map.rowMapperFor(Integer.class)).isNull();
		assertThat(map.rowMapperFor(String.class)).isNull();
	}
	
	@Test
	public void returnsNullResultSetExtractorForClassNotConfigured() {

		ResultSetExtractor resultSetExtractor = mock(ResultSetExtractor.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerResultSetExtractor(Number.class, resultSetExtractor);

		assertThat(map.resultSetExtractorFor(Integer.class)).isNull();
		assertThat(map.resultSetExtractorFor(String.class)).isNull();
	}

	@Test
	public void returnsInstanceRegisteredForSubClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerRowMapper(String.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}
	
	@Test
	public void returnsInstanceOfResultSetExtractorRegisteredForSubClass() {

		ResultSetExtractor resultSetExtractor = mock(ResultSetExtractor.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration().registerResultSetExtractor(String.class, resultSetExtractor);

		assertThat(map.resultSetExtractorFor(Object.class)).isEqualTo(resultSetExtractor);
	}

	@Test
	public void prefersExactTypeMatchClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerRowMapper(Object.class, mock(RowMapper.class)) //
				.registerRowMapper(Integer.class, rowMapper) //
				.registerRowMapper(Number.class, mock(RowMapper.class));

		assertThat(map.rowMapperFor(Integer.class)).isEqualTo(rowMapper);
	}
	
	@Test
	public void prefersExactResultSetExtractorTypeMatchClass() {

		ResultSetExtractor resultSetExtractor = mock(ResultSetExtractor.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerResultSetExtractor(Object.class, mock(ResultSetExtractor.class)) //
				.registerResultSetExtractor(Integer.class, resultSetExtractor) //
				.registerResultSetExtractor(Number.class, mock(ResultSetExtractor.class));

		assertThat(map.resultSetExtractorFor(Integer.class)).isEqualTo(resultSetExtractor);
	}

	@Test
	public void prefersLatestRegistrationForSuperTypeMatch() {

		RowMapper rowMapper = mock(RowMapper.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerRowMapper(Integer.class, mock(RowMapper.class)) //
				.registerRowMapper(Number.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}
	
	@Test
	public void prefersLatestRegistrationOfResultSetExtractorForSuperTypeMatch() {

		ResultSetExtractor resultSetExtractor = mock(ResultSetExtractor.class);

		QueryMappingConfiguration map = new DefaultQueryMappingConfiguration() //
				.registerResultSetExtractor(Integer.class, mock(ResultSetExtractor.class)) //
				.registerResultSetExtractor(Number.class, resultSetExtractor);

		assertThat(map.resultSetExtractorFor(Object.class)).isEqualTo(resultSetExtractor);
	}
}
