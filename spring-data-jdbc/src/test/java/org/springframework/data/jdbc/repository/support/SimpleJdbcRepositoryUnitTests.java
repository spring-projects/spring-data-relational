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
package org.springframework.data.jdbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

/**
 * Unit tests for {@link SimpleJdbcRepository}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
public class SimpleJdbcRepositoryUnitTests {

	@Mock JdbcAggregateOperations operations;
	@Mock RelationalPersistentEntity<Sample> entity;
	@Mock JdbcConverter converter;

	@Test // DATAJDBC-252
	public void saveReturnsEntityProducedByOperations() {

		SimpleJdbcRepository<Sample, Object> repository = new SimpleJdbcRepository<>(operations, entity, converter);

		Sample expected = new Sample();
		doReturn(expected).when(operations).save(any());

		assertThat(repository.save(new Sample())).isEqualTo(expected);
	}

	static class Sample {}
}
