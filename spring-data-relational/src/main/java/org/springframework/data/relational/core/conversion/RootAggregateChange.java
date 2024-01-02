/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

/**
 * Represents the change happening to the aggregate (as used in the context of Domain Driven Design) as a whole.
 *
 * @author Chirag Tailor
 * @since 3.0
 */
public interface RootAggregateChange<T> extends MutableAggregateChange<T> {

	/**
	 * The root object to which this {@link AggregateChange} relates. Guaranteed to be not {@code null}.
	 */
	T getRoot();

	/**
	 * Set the root object of the {@code AggregateChange}.
	 *
	 * @param aggregateRoot must not be {@literal null}.
	 */
	void setRoot(T aggregateRoot);

	/**
	 * Sets the action for the root object of this {@code AggregateChange}.
	 *
	 * @param action must not be {@literal null}.
	 */
	void setRootAction(DbAction.WithRoot<T> action);
}
