/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Table;

import java.util.function.BiFunction;

/**
 * Generates SQL statements for loading aggregates.
 *
 * @author Jens Schauder
 * @since 3.2
 */
public interface SqlGenerator {
	String findAll();

	String findById();

	String findAllById();

	String findAllByCondition(BiFunction<Table, RelationalPersistentEntity, Condition> conditionSource);

	AliasFactory getAliasFactory();
}
