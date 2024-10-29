/*
 * Copyright 2023-2024 the original author or authors.
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
package org.springframework.data.relational.core.sqlgeneration;


import java.util.List;

import org.junit.platform.commons.annotation.Testable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.BenchmarkSettings;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;

/**
 * Benchmark for {@link SingleQuerySqlGenerator}.
 *
 * @author Mark Paluch
 */
@Testable
public class SingleQuerySqlGeneratorBenchmark extends BenchmarkSettings {

	@Benchmark
	public String findAll(StateHolder state) {
		return new SingleQuerySqlGenerator(state.context, state.aliasFactory, PostgresDialect.INSTANCE).findAll(state.persistentEntity, null);
	}

	@State(Scope.Benchmark)
	public static class StateHolder {

		RelationalMappingContext context = new RelationalMappingContext();

		RelationalPersistentEntity<?> persistentEntity;

		AliasFactory aliasFactory = new AliasFactory();

		@Setup
		public void setup() {
			persistentEntity = context.getRequiredPersistentEntity(SingleReferenceAggregate.class);
		}
	}

	record TrivialAggregate(@Id Long id, String name) {
	}

	record SingleReferenceAggregate(@Id Long id, String name, List<TrivialAggregate> trivials) {
	}

}
