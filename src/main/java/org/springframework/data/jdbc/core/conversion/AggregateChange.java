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
package org.springframework.data.jdbc.core.conversion;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Jens Schauder
 * @since 1.0
 */
@RequiredArgsConstructor
@Getter
public class AggregateChange<T> {

	private final Kind kind;

	/** Type of the aggregate root to be changed */
	private final Class<T> entityType;

	/** Aggregate root, to which the change applies, if available */
	private final T entity;

	private final List<DbAction> actions = new ArrayList<>();

	public void executeWith(Interpreter interpreter) {
		actions.forEach(a -> a.executeWith(interpreter));
	}

	public void addAction(DbAction action) {
		actions.add(action);
	}

	public enum Kind {
		SAVE, DELETE
	}
}
