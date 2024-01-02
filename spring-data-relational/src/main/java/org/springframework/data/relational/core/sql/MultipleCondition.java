/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.relational.core.sql;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Wrapper for multiple {@link Condition}s.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public abstract class MultipleCondition extends AbstractSegment implements Condition {

	private final List<Condition> conditions;
	private final String delimiter;

	MultipleCondition(String delimiter, Condition... conditions) {

		super(conditions);

		this.delimiter = delimiter;
		this.conditions = Arrays.asList(conditions);
	}

	public List<Condition> getConditions() {
		return conditions;
	}

	@Override
	public String toString() {

		StringJoiner joiner = new StringJoiner(delimiter);
		conditions.forEach(c -> joiner.add(c.toString()));
		return joiner.toString();
	}
}
