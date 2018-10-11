/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.jdbc.core;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.relational.core.mapping.RelationalMappingContext;

/**
 * Provides {@link SqlGenerator}s per domain type. Instances get cached, so when asked multiple times for the same domain
 * type, the same generator will get returned.
 *
 * @author Jens Schauder
 */
@RequiredArgsConstructor
public class SqlGeneratorSource {

	private final Map<Class, SqlGenerator> sqlGeneratorCache = new HashMap<>();
	private final RelationalMappingContext context;

	SqlGenerator getSqlGenerator(Class<?> domainType) {

		return sqlGeneratorCache.computeIfAbsent(domainType,
				t -> new SqlGenerator(context, context.getRequiredPersistentEntity(t), this));

	}
}
