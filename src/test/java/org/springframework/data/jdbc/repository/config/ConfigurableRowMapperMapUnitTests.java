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
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.jdbc.core.RowMapper;

/**
 * Unit tests for {@link ConfigurableRowMapperMap}.
 * 
 * @author Jens Schauder
 */
public class ConfigurableRowMapperMapUnitTests {

	@Test
	public void freshInstanceReturnsNull() {

		RowMapperMap map = new ConfigurableRowMapperMap();

		assertThat(map.rowMapperFor(Object.class)).isNull();
	}

	@Test
	public void returnsConfiguredInstanceForClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		RowMapperMap map = new ConfigurableRowMapperMap().register(Object.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}

	@Test
	public void returnsNullForClassNotConfigured() {

		RowMapper rowMapper = mock(RowMapper.class);

		RowMapperMap map = new ConfigurableRowMapperMap().register(Number.class, rowMapper);

		assertThat(map.rowMapperFor(Integer.class)).isNull();
		assertThat(map.rowMapperFor(String.class)).isNull();
	}

	@Test
	public void returnsInstanceRegisteredForSubClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		RowMapperMap map = new ConfigurableRowMapperMap().register(String.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}

	@Test
	public void prefersExactTypeMatchClass() {

		RowMapper rowMapper = mock(RowMapper.class);

		RowMapperMap map = new ConfigurableRowMapperMap() //
				.register(Object.class, mock(RowMapper.class)) //
				.register(Integer.class, rowMapper) //
				.register(Number.class, mock(RowMapper.class));

		assertThat(map.rowMapperFor(Integer.class)).isEqualTo(rowMapper);
	}

	@Test
	public void prefersLatestRegistrationForSuperTypeMatch() {

		RowMapper rowMapper = mock(RowMapper.class);

		RowMapperMap map = new ConfigurableRowMapperMap() //
				.register(Integer.class, mock(RowMapper.class)) //
				.register(Number.class, rowMapper);

		assertThat(map.rowMapperFor(Object.class)).isEqualTo(rowMapper);
	}
}
