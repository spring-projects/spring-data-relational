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
package org.springframework.data.jdbc.core.mapping;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntityImpl;
import org.springframework.data.jdbc.core.mapping.Table;

/**
 * Unit tests for {@link JdbcPersistentEntityImpl}.
 * 
 * @author Oliver Gierke
 * @author Kazuki Shimizu
 */
public class JdbcPersistentEntityImplUnitTests {

	JdbcMappingContext mappingContext = new JdbcMappingContext();

	@Test // DATAJDBC-106
	public void discoversAnnotatedTableName() {

		JdbcPersistentEntity<?> entity = mappingContext.getPersistentEntity(DummySubEntity.class);

		assertThat(entity.getTableName()).isEqualTo("dummy_sub_entity");
	}

	@Table("dummy_sub_entity")
	static class DummySubEntity {}
}
