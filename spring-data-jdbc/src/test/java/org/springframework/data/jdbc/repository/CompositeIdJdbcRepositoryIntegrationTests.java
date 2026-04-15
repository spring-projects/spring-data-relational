/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.jdbc.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.testing.IntegrationTest;
import org.springframework.data.jdbc.testing.TestConfiguration;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;

/**
 * Integration tests for JDBC repositories with a composite {@link Id}.
 * <p>
 *
 * @author Christoph Strobl
 */
@IntegrationTest
class CompositeIdJdbcRepositoryIntegrationTests {

	@Autowired WithCompositeIdRepository repository;

	@Autowired JdbcAggregateOperations jdbcAggregateOperations;

	@Test // GH-2276
	void findAllByCompositePkNotInLooksRowsUpCorrectly() {

		this.jdbcAggregateOperations.insert(new WithCompositeId(new CompositeId(42, "HBAR"), "Walter"));
		this.jdbcAggregateOperations.insert(new WithCompositeId(new CompositeId(23, "2PI"), "Jesse"));
		this.jdbcAggregateOperations.insert(new WithCompositeId(new CompositeId(42, "2PI"), "Extra"));

		List<WithCompositeId> rows = this.repository.findAllByPkNotIn(
				List.of(new CompositeId(42, "HBAR"), new CompositeId(23, "2PI")));

		assertThat(rows).singleElement() //
				.satisfies(row -> {
					assertThat(row.name()).isEqualTo("Extra");
					assertThat(row.pk()).isEqualTo(new CompositeId(42, "2PI"));
				});
	}

	@Configuration
	@Import(TestConfiguration.class)
	@EnableJdbcRepositories(considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(value = WithCompositeIdRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	static class Config {
	}

	interface WithCompositeIdRepository extends CrudRepository<WithCompositeId, CompositeId> {

		List<WithCompositeId> findAllByPkNotIn(Collection<CompositeId> ids);
	}

	@Table("with_composite_id")
	record WithCompositeId(@Id @Embedded.Nullable CompositeId pk, @Column("NAME") String name) {
	}

	record CompositeId(@Column("col_one") Integer one, @Column("col_two") String two) {
	}
}
